module YotpoKafka
  class Consumer < YotpoKafka::BaseConsumer

    def initialize(params = {})
      super

      @producer = Producer.new(
        client_id: @group_id,
        logstash_logger: @use_logstash_logger,
        broker_url: @seed_brokers
      )
    end

    def handle_consume(payload, message)
      consume_message(payload)
    rescue => error
      handle_consume_error(message, error)
    end

    def handle_consume_error(message, error)
      if @failures_topic.nil?
        log_error('Consume failure - not handling retry due to consumer policy',
                  error: error.message,
                  topic: message.topic,
                  backtrace: error.backtrace)
        return
      end
      log_error('Consume failure - handling retry',
                error: error.message,
                topic: message.topic,
                backtrace: error.backtrace)

      unless message.headers[YotpoKafka.retry_header_key]
        message.headers[YotpoKafka.retry_header_key] = get_init_retry_header(message.topic, error)
      end
      retry_hdr = update_retry_header(message.headers[YotpoKafka.retry_header_key], error)
      publish_to_retry_service(retry_hdr, message)
    end

    def extract_payload(message)
      message.value
    end

    def get_init_retry_header(topic, error)
      {
        CurrentAttempt: @num_retries,
        NextExecTime: (Time.now.utc + @seconds_between_retries).to_datetime.rfc3339,
        Error: error.message,
        Backtrace: error.backtrace,
        MainTopic: topic,
        FailuresTopic: @failures_topic.nil? ?  get_fail_topic_name(topic) : @failures_topic,
      }.to_json
    end

    def update_retry_header(retry_hdr, error)
      parsed_hdr = JSON.parse(retry_hdr)
      {
        CurrentAttempt: parsed_hdr['CurrentAttempt'] - 1,
        NextExecTime: (Time.now.utc + @seconds_between_retries).to_datetime.rfc3339,
        Error: error.message,
        Backtrace: error.backtrace,
        MainTopic: parsed_hdr['MainTopic'],
        FailuresTopic: parsed_hdr['FailuresTopic'],
      }
    end

    def publish_to_retry_service(retry_hdr, message)
      if (retry_hdr[:CurrentAttempt]).positive?
        message.headers[YotpoKafka.retry_header_key] = retry_hdr.to_json
        topic = YotpoKafka.retry_topic
        log_error('Message failed to consumed, send to RETRY', retry_hdr: retry_hdr.to_s)
      else
        retry_hdr[:NextExecTime] = Time.now.utc.to_datetime.rfc3339
        message.headers[YotpoKafka.retry_header_key] = retry_hdr.to_json
        topic = YotpoKafka.fatal_topic
        log_error('Message failed to consumed, send to FATAL', retry_hdr: retry_hdr.to_s)
      end

      @producer.publish(topic, message.value, message.headers, message.key, false)
    end
  end
end
