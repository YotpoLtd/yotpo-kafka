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
      @consumer = YotpoKafka.kafka.consumer(group_id: @group_id)
      trap('TERM') { @consumer.stop }
      @producer = Producer.new(
        client_id: @group_id,
        logstash_logger: true
      )
    rescue => error
      log_error('Consumer Could not initialize',
                exception: error.message,
                broker_url: YotpoKafka.seed_brokers)
      raise 'Could not initialize'
    end

    def start_consumer
      log_info('Starting consume', broker_url: YotpoKafka.seed_brokers)
      subscribe_to_topics
      @consumer.each_message do |message|
        @consumer.mark_message_as_processed(message)
        @consumer.commit_offsets
        handle_consume(message)
      end
    rescue => error
      log_error('Consumer failed to start: ' + error.message,
                exception: error.message,
                broker_url: YotpoKafka.seed_brokers)
    end

    def handle_consume(message)
      if YotpoKafka.include_headers
        consume_with_headers(message)
      else
        consume_without_headers(message)
      end
    end

    def consume_with_headers(message)
      log_info('Start handling consume with headers',
               payload: message.value, headers: message.headers, topic: message.topic, broker_url: YotpoKafka.seed_brokers)
      consume_message(message.value)
      RedCross.monitor_track(event: 'messageConsumed', properties: { success: true }) if @red_cross
    rescue => error
      RedCross.monitor_track(event: 'messageConsumed', properties: { success: false }) if @red_cross
      log_error('Consume error: ' + error.message,
                topic: message.topic, payload: message.value, backtrace: error.backtrace)
      handle_error_with_headers(message, error)
    end

    def consume_without_headers(message)
      log_info('Start handling consume without headers',
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
      log_error('Consume parse error - no retry: ' + parse_error.to_s,
                topic: message.topic,
                payload: message.value)
      RedCross.monitor_track(event: 'messageConsumed', properties: { success: false }) if @red_cross
    rescue => error
      log_error('Consume error: ' + error.message, topic: message.topic, backtrace: error.backtrace)
      RedCross.monitor_track(event: 'messageConsumed', properties: { success: false }) if @red_cross
      handle_error_without_headers(parsed_payload, message.topic, message.key, error)
    end

    def subscribe_to_topics
      @topics.each do |t|
        @consumer.subscribe(t)
        log_info('Consumer subscribes to topic: ' + t)
        next unless @listen_to_failures

        failure_topic = get_fail_topic_name(t)
        begin
          YotpoKafka.kafka.create_topic(failure_topic,
                                        num_partitions: YotpoKafka.default_partitions_num,
                                        replication_factor: YotpoKafka.default_replication_factor)
          log_info('Created new topic: ' + failure_topic)
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
      main_topic + '.' + group + '.failures'
    end

    def handle_error_without_headers(payload, topic, key, error)
      if payload[YotpoKafka.retry_header_key].nil?
        payload[YotpoKafka.retry_header_key] = {
          CurrentAttempt: @num_retries,
          NextExecTime: (Time.now.utc + @seconds_between_retries).to_datetime.rfc3339,
          Error: error.to_s,
          MainTopic: topic,
          FailuresTopic: get_fail_topic_name(topic)
        }.to_json
      end
      parsed_hdr = JSON.parse(payload[YotpoKafka.retry_header_key])
      retry_hdr = {
        CurrentAttempt: parsed_hdr['CurrentAttempt'] - 1,
        NextExecTime: (Time.now.utc + @seconds_between_retries).to_datetime.rfc3339,
        Error: error.to_s,
        MainTopic: parsed_hdr['MainTopic'],
        FailuresTopic: parsed_hdr['FailuresTopic']
      }
      if (retry_hdr[:CurrentAttempt]).positive?
        payload[YotpoKafka.retry_header_key] = retry_hdr.to_json.to_s
        log_info('Message failed to consumed, send to RETRY',
                 topic: topic,
                 retry_hdr: retry_hdr.to_s)
        if @seconds_between_retries.zero?
          @producer.publish(parsed_hdr['FailuresTopic'], payload, {}, key)
        else
          @producer.publish(YotpoKafka.retry_topic, payload, {}, key)
        end
      else
        retry_hdr[:NextExecTime] = Time.now.utc.to_datetime.rfc3339
        payload[YotpoKafka.retry_header_key] = retry_hdr.to_json
        log_info('Message failed to consumed, sent to FATAL',
                 topic: topic,
                 retry_hdr: retry_hdr.to_s)
        @producer.publish(YotpoKafka.fatal_topic, payload, {}, key)
      end
    end

    def handle_error_with_headers(message, error)
      unless message.headers[YotpoKafka.retry_header_key]
        message.headers[YotpoKafka.retry_header_key] = {
          CurrentAttempt: @num_retries,
          NextExecTime: (Time.now.utc + @seconds_between_retries).to_datetime.rfc3339,
          Error: error.to_s,
          MainTopic: message.topic,
          FailuresTopic: get_fail_topic_name(message.topic)
        }.to_json
      end
      parsed_hdr = JSON.parse(message.headers[YotpoKafka.retry_header_key])
      retry_hdr = {
        CurrentAttempt: parsed_hdr['CurrentAttempt'] - 1,
        NextExecTime: (Time.now.utc + @seconds_between_retries).to_datetime.rfc3339,
        Error: error.to_s,
        MainTopic: parsed_hdr['MainTopic'],
        FailuresTopic: parsed_hdr['FailuresTopic']
      }
      if (retry_hdr[:CurrentAttempt]).positive?
        message.headers[YotpoKafka.retry_header_key] = retry_hdr.to_json
        log_info('Message failed to consumed, wait for RETRY',
                 topic: message.topic,
                 retry_hdr: retry_hdr.to_s)
        if @seconds_between_retries.zero?
          @producer.publish(parsed_hdr['FailuresTopic'], message.value, message.headers, message.key)
        else
          @producer.publish(YotpoKafka.retry_topic, message.value, message.headers, message.key)
        end
      else
        retry_hdr[:NextExecTime] = Time.now.utc.to_datetime.rfc3339
        message.headers[YotpoKafka.retry_header_key] = retry_hdr.to_json
        log_info('Message failed to consumed, sent to FATAL',
                 topic: message.topic,
                 retry_hdr: retry_hdr.to_s)
        @producer.publish(YotpoKafka.fatal_topic, message.value, message.headers, message.key)
      end
    end

    def consume_message(_message)
      raise NotImplementedError
    end
  end
end
