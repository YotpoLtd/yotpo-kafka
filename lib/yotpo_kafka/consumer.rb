require 'avro_turf/messaging'
require 'kafka'
require 'ylogger'

module YotpoKafka
  class Consumer
    extend Ylogger

    def initialize(params = {})
      YotpoKafka::YLoggerKafka.config(true)
      set_log_tag(:yotpo_ruby_kafka)
      @seconds_between_retries = params[:seconds_between_retries] || 0
      @listen_to_failures = true
      @listen_to_failures = params[:listen_to_failures] unless params[:listen_to_failures].nil?
      @num_retries = params[:num_retries] || 0
      @topics = Array(params[:topics]) || nil
      @red_cross = params[:red_cross] || false
      @group_id = params[:group_id] || 'missing_groupid'
      @avro_encoding = params[:avro_encoding] || false
      @avro = nil
      @consumer = YotpoKafka.kafka.consumer(group_id: @group_id)
      trap('TERM') { @consumer.stop }
      @producer = Producer.new(
        client_id: @group_id,
        avro_encoding: @avro_encoding
      )
    rescue => error
      log_error('Consumer Could not initialize',
                error: error.message,
                broker_url: YotpoKafka.seed_brokers,
                backtrace: error.backtrace)
      raise 'Could not initialize'
    end

    def set_avro_registry(registry_url)
      @avro = AvroTurf::Messaging.new(registry_url: registry_url)
      @producer.set_avro_registry(registry_url)
    end

    def start_consumer
      log_info('Starting consume', broker_url: YotpoKafka.seed_brokers)
      subscribe_to_topics
      @consumer.each_message do |message|
        @consumer.mark_message_as_processed(message)
        @consumer.commit_offsets
        if @avro_encoding
          raise 'avro schema is not set' unless @avro

          schema = message.topic
          if schema.include? YotpoKafka.failures_topic_suffix
            if YotpoKafka.kafka_v2
              message.headers[YotpoKafka.retry_header_key]['MainTopic']
            else
              JSON.parse(message.value)['MainTopic']
            end
          end
          message.value = @avro.decode(message.value, schema_name: schema)
        end
        handle_consume(message)
      end
    rescue => error
      log_error('Consumer failed to start: ' + error.message,
                error: error.message,
                backtrace: error.backtrace,
                topics: @topics,
                group: @group_id,
                broker_url: YotpoKafka.seed_brokers)
    end

    def handle_consume(message)
      if YotpoKafka.kafka_v2
        consume_kafka_v2(message)
      else
        consume_kafka_v1(message)
      end
    end

    def consume_kafka_v2(message)
      log_info('Start handling consume',
               payload: message.value, headers: message.headers, topic: message.topic, broker_url: YotpoKafka.seed_brokers)
      consume_message(message.value)
      RedCross.monitor_track(event: 'messageConsumed', properties: { success: true }) if @red_cross
    rescue => error
      RedCross.monitor_track(event: 'messageConsumed', properties: { success: false }) if @red_cross
      log_error('Consume error: ' + error.message,
                topic: message.topic, payload: message.value, backtrace: error.backtrace)
      handle_error_kafka_v2(message, error)
    end

    def consume_kafka_v1(message)
      log_info('Start handling consume',
               payload: message.value, topic: message.topic, broker_url: YotpoKafka.seed_brokers)
      parsed_payload = JSON.parse(message.value)
      unless parsed_payload.is_a?(Hash)
        # can happen if value is single number
        raise JSON::ParserError.new('Parsing payload to json failed')
      end

      consume_message(parsed_payload)
      log_info('Message consumed and handled', topic: message.topic)
      RedCross.monitor_track(event: 'messageConsumed', properties: { success: true }) if @red_cross
    rescue JSON::ParserError => parse_error
      log_info('Consume json parse error - proceeding without conversion: ' + parse_error.to_s,
               topic: message.topic,
               payload: message.value)
      RedCross.monitor_track(event: 'messageConsumed', properties: { success: true }) if @red_cross
    rescue => error
      log_error('Consume error: ' + error.message, topic: message.topic, backtrace: error.backtrace)
      RedCross.monitor_track(event: 'messageConsumed', properties: { success: false }) if @red_cross
      handle_error_kafka_v1(parsed_payload, message.topic, message.key, error)
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
          YotpoKafka.kafka.create_topic(failure_topic,
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

    def get_init_retry_header(topic, error)
      {
        CurrentAttempt: @num_retries,
        NextExecTime: (Time.now.utc + @seconds_between_retries).to_datetime.rfc3339,
        Error: error.message,
        MainTopic: topic,
        FailuresTopic: get_fail_topic_name(topic)
      }.to_json
    end

    def update_retry_header(retry_hdr, error)
      parsed_hdr = JSON.parse(retry_hdr)
      {
        CurrentAttempt: parsed_hdr['CurrentAttempt'] - 1,
        NextExecTime: (Time.now.utc + @seconds_between_retries).to_datetime.rfc3339,
        Error: error.message,
        MainTopic: parsed_hdr['MainTopic'],
        FailuresTopic: parsed_hdr['FailuresTopic']
      }
    end

    def publish_to_retry_service(retry_hdr, message, kafka_v2, key)
      if (retry_hdr[:CurrentAttempt]).positive?
        set_headers(message, retry_hdr, kafka_v2)
        log_info('Message failed to consumed, send to RETRY',
                 retry_hdr: retry_hdr.to_s)
        if @seconds_between_retries.zero?
          publish_based_on_version(parsed_hdr['FailuresTopic'], message, kafka_v2, key)
        else
          publish_based_on_version(YotpoKafka.retry_topic, message, kafka_v2, key)
        end
      else
        retry_hdr[:NextExecTime] = Time.now.utc.to_datetime.rfc3339
        set_headers(message, retry_hdr, kafka_v2)
        log_info('Message failed to consumed, sent to FATAL',
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
        @producer.publish(topic, message.value, message.headers, message.key)
      else
        @producer.publish(topic, message, {}, key)
      end
    end

    def handle_error_kafka_v1(payload, topic, key, error)
      payload[YotpoKafka.retry_header_key] = get_init_retry_header(topic, error) if payload[YotpoKafka.retry_header_key].nil?
      retry_hdr = update_retry_header(payload[YotpoKafka.retry_header_key], error)
      publish_to_retry_service(retry_hdr, payload, false, key)
    end

    def handle_error_kafka_v2(message, error)
      unless message.headers[YotpoKafka.retry_header_key]
        message.headers[YotpoKafka.retry_header_key] = get_init_retry_header(message.topic, error)
      end
      retry_hdr = update_retry_header(message.headers[YotpoKafka.retry_header_key], error)
      publish_to_retry_service(retry_hdr, message, true, message.key)
    end

    def consume_message(_message)
      raise NotImplementedError
    end
  end
end
