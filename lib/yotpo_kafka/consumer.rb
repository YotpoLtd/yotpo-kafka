require 'kafka'
require 'phobos/cli/runner'
require 'ylogger'

module YotpoKafka
  class Consumer
    include ::Phobos::Handler
    extend Ylogger

    def initialize()
      @gap_between_retries = 0
      @num_retries = 0
      @logger = nil
      @use_red_cross = nil
    end

    def self.start_consumer(params = {})
      config(params)
      log_info("Configured successfully")
    rescue => error
      log_error("Could not subscribe as a consumer",
                { handler: params[:handler],
                          error: error})
      end
    end

    def self.config(params)
      YotpoKafka::RedCrossKafka.config(params[:red_cross]) unless params[:red_cross].nil?
      YotpoKafka::YLoggerKafka.config(params[:logstash_logger] || false)
      YotpoKafka::ConsumerRunner.run(params)
    end

    def consume_message(_message)
      raise NotImplementedError
    end

    def consume(payload, metadata)
      parsed_payload = JSON.parse(payload)
      consume_message(parsed_payload['message'])
      log_info( "Message consumed", { topic: metadata[:topic],
                                      handler: metadata[:handler]})
      RedCross.monitor_track(event: 'messageConsumed', properties: { success: true }) unless @use_red_cross.nil?
    rescue => error
      log_error("Message was not consumed", {topic: metadata.topic,
                                             handler: metadata[:handler],
                                             error: error})
      enqueue_to_relevant_topic(JSON.parse(payload), error, metadata) unless @num_retries == -1
      RedCross.monitor_track(event: 'messageConsumed', properties: { success: false }) unless @use_red_cross.nil?
    end

    def enqueue(payload, topic, error)
      Resque.enqueue_in(@gap_between_retries, ConsumerWorker,
                        exception_message: error,
                        topic: topic,
                        base64_payload: Base64.encode64(payload.to_json),
                        kafka_broker_url: payload['kafka_header']['kafka_broker_url'])
    rescue => error
      log_error("Enqueue failed", {error: error})
    end

    def enqueue_to_relevant_topic(payload, error, metadata)
      calc_num_of_retries(payload)
      topic_to_enqueue = get_topic_to_enqueue(payload, metadata)
      enqueue(payload, topic_to_enqueue, error) unless topic_to_enqueue.nil?
    end

    def calc_num_of_retries(payload)
      payload['kafka_header']['num_retries'] = if payload['kafka_header']['num_retries'].nil? then
                                           @num_retries
                                         else
                                           payload['kafka_header']['num_retries'] - 1
                                         end
    end

    def get_topic_to_enqueue(payload, metadata)
      if payload['kafka_header']['num_retries'] > 0
        topic_of_failures = "#{metadata[:topic]}_#{metadata[:group_id]}_failures"
        return topic_of_failures
      elsif payload['kafka_header']['num_retries'] == 0
        topic_of_fatal = "#{metadata[:topic]}_fatal"
        return topic_of_fatal
      else
        return nil
      end
    end
end
