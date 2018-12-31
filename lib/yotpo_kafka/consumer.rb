require 'kafka'
require 'phobos/cli/runner'
require 'ylogger'

module YotpoKafka
  class Consumer
    include ::Phobos::Handler
    extend Ylogger

    def initialize(context = {})
      params = HashWithIndifferentAccess.new(context)
      @gap_between_retries = params['gap_between_retries'] || 0
      @num_retries = params['num_retries'] || 0
      @red_cross_params = params['red_cross_params'] || nil
      @logstash_logger = params['logstash_logger'] || false
      @active_job = params['active_job'] || nil
      config()
    rescue => error
      log_error("Could not initialize", error)
    end

    def self.start_consumer(params)
      YotpoKafka::ConsumerRunner.run(params)
      YotpoKafka::YLoggerKafka.config(params[:logstash_logger] || false)
      log_info("Configured successfully")
    rescue => error
      log_error("Could not subscribe as a consumer",{ handler: params[:handler].to_s}, exception: error)
    end

    def config()
      YotpoKafka::RedCrossKafka.config(@red_cross_params)
      YotpoKafka::YLoggerKafka.config(@logstash_logger)
      YotpoKafka::ActiveJobs.config(@active_job)
    rescue => error
      log_error("Could not config", exception: error)
    end

    def consume_message(_message)
      raise NotImplementedError
    end

    def consume(payload, metadata)
      parsed_payload = JSON.parse(payload)
      consume_message(parsed_payload['message'])
      log_info( "Message consumed", { topic: metadata[:topic],
                                      handler: metadata[:handler].to_s})
      RedCross.monitor_track(event: 'messageConsumed', properties: { success: true }) unless @use_red_cross.nil?
    rescue => error
      log_error("Message was not consumed", {topic: metadata[:topic],
                                             handler: metadata[:handler].to_s}, exception: error)
      enqueue_to_relevant_topic(JSON.parse(payload), error, metadata) unless @num_retries == -1
      RedCross.monitor_track(event: 'messageConsumed', properties: { success: false }) unless @use_red_cross.nil?
    end

    def enqueue(payload, topic, error)
      params = {
          'exception_message' => error,
          'topic' => topic,
          'base64_payload' => Base64.encode64(payload.to_json),
          'kafka_broker_url' => payload['kafka_header']['kafka_broker_url'],
          'active_job' => @active_job,
          'red_cross_params' => @red_cross_params,
          'logstash_logger' => @logstash_logger}

      ConsumerWorker.set(wait: @gap_between_retries).perform_later(params.to_json)
    rescue => error
      log_error("Enqueue failed", exception: error)
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
end
