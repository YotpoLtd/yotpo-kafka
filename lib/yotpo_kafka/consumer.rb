require 'kafka'
require 'ylogger'

module YotpoKafka
  class Consumer
    extend Ylogger

    attr_accessor :kafka

    def initialize(params = {})
      use_logstash_logger = params[:logstash_logger] != false
      YotpoKafka::YLoggerKafka.config(use_logstash_logger)
      set_log_tag(:yotpo_ruby_kafka)
      @seed_brokers = params[:broker_url] || ENV['BROKER_URL'] || '127.0.0.1:9092'
      @kafka = Kafka.new(@seed_brokers)
      @seconds_between_retries = params[:seconds_between_retries] || 0
      @listen_to_failures = true
      @listen_to_failures = params[:listen_to_failures] unless params[:listen_to_failures].nil?
      @num_retries = params[:num_retries] || 0
      @topics = Array(params[:topics]) || nil
      @group_id = params[:group_id] || 'missing_groupid'
      @avro_encoding = params[:avro_encoding] || false
      @avro = nil
      @json_parse = params[:json_parse].nil? ? true : params[:json_parse]
      @consumer = @kafka.consumer(group_id: @group_id)
      @producer = Producer.new(
        client_id: @group_id,
        avro_encoding: @avro_encoding,
        logstash_logger: use_logstash_logger,
        broker_url: @seed_brokers
      )
    rescue => error
      log_error('Consumer Could not initialize',
                error: error.message,
                broker_url: @seed_brokers,
                backtrace: error.backtrace)
      raise 'Could not initialize'
    end

    def set_avro_registry(registry_url)
      require 'avro_turf/messaging'
      @avro = AvroTurf::Messaging.new(registry_url: registry_url)
    end

    def start_consumer
      log_debug('Starting consume', broker_url: @seed_brokers)
      subscribe_to_topics
      @consumer.each_message do |message|
        @consumer.mark_message_as_processed(message)
        @consumer.commit_offsets
        # remember that we got something with key and ignore reties for same key
        payload = message.value
        if @avro_encoding
          unless from_failure_topic(message.topic) && !YotpoKafka.kafka_v2
            raise 'avro schema is not set' unless @avro

            payload = @avro.decode(payload).to_json
          end
        end
        handle_consume(payload, message)
      end
    rescue => error
      log_error('Consumer failed to start: ' + error.message,
                error: error.message,
                backtrace: error.backtrace,
                topics: @topics,
                group: @group_id,
                broker_url: @seed_brokers)
    end

    def from_failure_topic(topic)
      topic.include? YotpoKafka.failures_topic_suffix
    end

    def handle_consume(payload, message)
      if YotpoKafka.kafka_v2
        consume_kafka_v2(payload, message)
      else
        consume_kafka_v1(payload, message)
      end
    end

    def get_printed_payload(payload)
      payload.to_s.force_encoding('UTF-8')
    rescue => error
      log_error('kafka_v1 encoding error: ' + error.message, backtrace: error.backtrace)
    end

    def consume_kafka_v2(payload, message)
      print_payload = get_printed_payload(payload)
      log_debug('Start handling consume',
                payload: print_payload, headers: message.headers, topic: message.topic, broker_url: @seed_brokers)
      consume_message(payload)
    rescue => error
      log_error('consume_kafka_v2 failed in service - handling retry: ' + error.message,
                topic: message.topic, payload: print_payload, backtrace: error.backtrace)
      handle_error_kafka_v2(message, error)
    rescue SignalException => error
      log_error('Signal Exception sending message to retry in kafka v2 and closing server, error: ' + error.message,
                topic: message.topic, payload: print_payload, backtrace: error.backtrace)
      handle_error_kafka_v2(message, error)
      @consumer.stop
    end

    def consume_kafka_v1(payload, message)
      print_payload = get_printed_payload(payload)
      log_info('Start handling consume',
               topic: message.topic, broker_url: @seed_brokers, payload: print_payload)
      if @json_parse
        begin
          payload = JSON.parse(payload)
          unless payload.is_a?(Hash)
            # can happen if value is single number
            raise JSON::ParserError.new('Parsing payload to json failed')
          end
        rescue JSON::ParserError => parse_error
          log_error('Consume kafka_v1, json parse error: ' + parse_error.to_s,
                    topic: message.topic)
        end
      end
      consume_message(payload)
      log_debug('Message consumed and handled', topic: message.topic)
    rescue => error
      log_error('consume_kafka_v1 failed in service - handle retry: ' + error.message,
                topic: message.topic, backtrace: error.backtrace)
      log_error('consume_kafka_v1 failed in service - printed payload: ' + print_payload,
                topic: message.topic, backtrace: error.backtrace)
      handle_error_kafka_v1(payload, message.topic, message.key, error)
    rescue SignalException => error
      log_error('Signal Exception sending message to retry in kafka v1 and closing server, error: ' + error.message,
                topic: message.topic, backtrace: error.backtrace)
      handle_error_kafka_v1(payload, message.topic, message.key, error)
      @consumer.stop
    end

    def subscribe_to_topics
      @topics.each do |t|
        @consumer.subscribe(t)
        log_info('Consumer subscribes to topic: ' + t)
        next unless @listen_to_failures

        failure_topic = get_fail_topic_name(t)
        begin
          log_info('Created new topic: ' + failure_topic,
                   partitions_num: YotpoKafka.default_partitions_num,
                   replication_factor: YotpoKafka.default_replication_factor)
          @kafka.create_topic(failure_topic,
                              num_partitions: YotpoKafka.default_partitions_num.to_i,
                              replication_factor: YotpoKafka.default_replication_factor.to_i)
        rescue Kafka::TopicAlreadyExists
          nil
        end

        @consumer.subscribe(failure_topic)
        log_info('Consume subscribes to topic: ' + failure_topic)
      end
    end

    def get_fail_topic_name(main_topic)
      main_topic.tr('.', '_')
      group = @group_id.tr('.', '_')
      main_topic + '.' + group + YotpoKafka.failures_topic_suffix
    end

    def get_init_retry_header(topic, key, error)
      {
        CurrentAttempt: @num_retries,
        NextExecTime: (Time.now.utc + @seconds_between_retries).to_datetime.rfc3339,
        Error: error.message,
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
        MainTopic: parsed_hdr['MainTopic'],
        FailuresTopic: parsed_hdr['FailuresTopic'],
        Key: parsed_hdr['Key']
      }
    end

    def publish_to_retry_service(retry_hdr, message, kafka_v2, key)
      if (retry_hdr[:CurrentAttempt]).positive?
        set_headers(message, retry_hdr, kafka_v2)
        log_error('Message failed to consumed, send to RETRY',
                  retry_hdr: retry_hdr.to_s, payload: get_printed_payload(message))
        if @seconds_between_retries.zero?
          publish_based_on_version(retry_hdr[:FailuresTopic], message, kafka_v2, key)
        else
          publish_based_on_version(YotpoKafka.retry_topic, message, kafka_v2, key)
        end
      else
        retry_hdr[:NextExecTime] = Time.now.utc.to_datetime.rfc3339
        set_headers(message, retry_hdr, kafka_v2)
        log_error('Message failed to consumed, sent to FATAL',
                  retry_hdr: retry_hdr.to_s, payload: get_printed_payload(message))
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
      log_info('handle_error_kafka_v1', topic: topic)
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

    def consume_message(_message)
      raise NotImplementedError
    end
  end
end
