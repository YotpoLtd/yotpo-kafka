module YotpoKafka
  class Consumer < YotpoKafka::BaseConsumer

    def initialize(params = {})
      super

      @producer = Producer.new(
        client_id: @group_id,
        avro_encoding: @avro_encoding,
        logstash_logger: @use_logstash_logger,
        broker_url: @seed_brokers
      )
    end

    def extract_payload(message)
      message.value
    end

    def handle_consume(payload, message)
      if YotpoKafka.kafka_v2
        consume_kafka_v2(payload, message)
      else
        consume_kafka_v1(payload, message)
      end
    end

    def consume_kafka_v2(payload, message)
      log_debug('Start handling consume',
                headers: message.headers,
                topic: message.topic,
                broker_url: @seed_brokers)
      consume_message(payload)
    rescue => error
      handle_error_kafka_v2(message, error)
    rescue SignalException => error
      log_error('Signal Exception',
                error: error.message,
                backtrace: error.backtrace)
      handle_error_kafka_v2(message, error)
      @consumer.stop
    end

    def consume_kafka_v1(payload, message)
      if @json_parse
        begin
          payload = JSON.parse(payload)
          unless payload.is_a?(Hash)
            # can happen if value is single number
            raise JSON::ParserError.new('Parsing payload to json failed')
          end
        rescue JSON::ParserError => parse_error
          log_error('Consume kafka_v1, json parse error',
                    error: parse_error.to_s,
                    topic: message.topic)
        end
      end
      consume_message(payload)
      log_debug('Message consumed and handled', topic: message.topic)
    rescue => error
      handle_error_kafka_v1(payload, message.topic, message.key, error)
    rescue SignalException => error
      log_error('Signal Exception',
                error: error.message,
                backtrace: error.backtrace)
      handle_error_kafka_v1(payload, message.topic, message.key, error)
      @consumer.stop
    end

    def get_init_retry_header(topic, key, error)
      {
        CurrentAttempt: @num_retries,
        NextExecTime: (Time.now.utc + @seconds_between_retries).to_datetime.rfc3339,
        Error: error.message,
        Backtrace: error.backtrace,
        MainTopic: topic,
        FailuresTopic: get_fail_topic_name(topic),
        Key: key
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
        Key: parsed_hdr['Key']
      }
    end

    def publish_to_retry_service(retry_hdr, message, kafka_v2, key)
      if (retry_hdr[:CurrentAttempt]).positive?
        set_headers(message, retry_hdr, kafka_v2)
        log_error('Message failed to consumed, send to RETRY',
                  retry_hdr: retry_hdr.to_s)
        if @seconds_between_retries.zero?
          publish_based_on_version(retry_hdr[:FailuresTopic], message, kafka_v2, key)
        else
          publish_based_on_version(YotpoKafka.retry_topic, message, kafka_v2, key)
        end
      else
        retry_hdr[:NextExecTime] = Time.now.utc.to_datetime.rfc3339
        set_headers(message, retry_hdr, kafka_v2)
        log_error('Message failed to consumed, sent to FATAL',
                  retry_hdr: retry_hdr.to_s)
        publish_based_on_version(YotpoKafka.fatal_topic, message, kafka_v2, key)
      end
    end

    def set_headers(message, retry_hdr, kafka_v2)
      if kafka_v2
        message.headers[YotpoKafka.retry_header_key] = retry_hdr.to_json
      else
        message[YotpoKafka.retry_header_key] = retry_hdr.to_json.to_s
      end
    end

    def publish_based_on_version(topic, message, kafka_v2, key = nil)
      if kafka_v2
        @producer.publish(topic, message.value, message.headers, key, false)
      else
        @producer.publish(topic, message, {}, key)
      end
    end

    def handle_error_kafka_v1(payload, topic, key, error)
      return unless @listen_to_failures

      begin
        key = key.to_s.encode('UTF-8') unless key.nil?
      rescue Encoding::UndefinedConversionError
        log_error('key sent in invalid format')
        return
      end
      payload[YotpoKafka.retry_header_key] = get_init_retry_header(topic, key, error) if payload[YotpoKafka.retry_header_key].nil?
      retry_hdr = update_retry_header(payload[YotpoKafka.retry_header_key], error)
      publish_to_retry_service(retry_hdr, payload, false, key)
    end

    def handle_error_kafka_v2(message, error)
      unless @listen_to_failures
        log_error('handle_error_kafka_v2 - not handling retry',
                  error: error.message,
                  topic: message.topic,
                  backtrace: error.backtrace)
        return
      end
      log_error('handle_error_kafka_v2 - handling retry',
                error: error.message,
                topic: message.topic,
                backtrace: error.backtrace)
      key = message.key || nil
      begin
        key = key.to_s.encode('UTF-8') unless key.nil?
      rescue Encoding::UndefinedConversionError
        log_error('key sent in invalid format')
        return
      end

      unless message.headers[YotpoKafka.retry_header_key]
        message.headers[YotpoKafka.retry_header_key] = get_init_retry_header(message.topic, key, error)
      end
      retry_hdr = update_retry_header(message.headers[YotpoKafka.retry_header_key], error)
      publish_to_retry_service(retry_hdr, message, true, key)
    end
  end
end
