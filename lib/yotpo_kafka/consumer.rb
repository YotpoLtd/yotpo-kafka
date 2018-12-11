require 'kafka'
require 'phobos/cli/runner'

module YotpoKafka
  class Consumer
    include ::Phobos::Handler
    ALL_CONSUME_FAILURES_FROM_ALL_TOPICS = 'all_consume_failures_from_all_topics'

    def initialize()
      @gap_between_retries = 0
      @topic_for_failures = ''
      @num_retries = -1
      @logger = nil
    end

    def self.start_consumer_running(params = {})
      YotpoKafka::ConsumerRunner.run(params)
    rescue => error
      if @logger
        @logger.error "Could not subscribe the following handler:
                       #{params[:handler].to_s} due to error: #{error}"
      end
    end

    def consume_message(_message)
      raise NotImplementedError
    end

    def consume(payload, metadata)
      parsed_payload = JSON.parse(payload)
      consume_message(parsed_payload['message'])
    rescue => error
      enqueue_to_relevant_topic(JSON.parse(payload), error) unless @num_retries == -1
      if @logger
        @logger.error "Could not consume from the following topic
                        #{metadata[:topic]} due to error: #{error}"
      end
    end

    def enqueue(payload, topic, error)
      Resque.enqueue_in(@gap_between_retries, ConsumerWorker,
                        exception_message: error,
                        topic: topic,
                        base64_payload: Base64.encode64(payload.to_json),
                        kafka_broker_url: payload['header']['kafka_broker_url'])
    rescue => error
      if @logger
        @logger.error "Could not enqueue to resque the following topic
                       #{topic} due to error: #{error}"
      end
    end

    def enqueue_to_relevant_topic(payload, error)
      calc_num_of_retries(payload)
      topic_to_enqueue = get_topic_to_enqueue(payload)
      enqueue(payload, topic_to_enqueue, error) unless topic_to_enqueue.nil?
    end

    def calc_num_of_retries(payload)
      payload['header']['num_retries'] = if payload['header']['num_retries'].nil? then
                                           @num_retries
                                         else
                                           payload['header']['num_retries'] - 1
                                         end
    end

    def get_topic_to_enqueue(payload)
      if payload['header']['num_retries'] > 0
        return @topic_for_failures
      elsif payload['header']['num_retries'] == 0
        return Consumer::ALL_CONSUME_FAILURES_FROM_ALL_TOPICS
      else
        return nil
      end
    end

  end
end