module YotpoKafka
  class BaseConsumer
    extend Ylogger

    attr_accessor :kafka

    def initialize(params = {})
      @use_logstash_logger = params[:logstash_logger] != false
      YotpoKafka::YLoggerKafka.config(@use_logstash_logger)
      set_log_tag(:yotpo_ruby_kafka)
      @seed_brokers = params[:broker_url] || ENV['BROKER_URL'] || '127.0.0.1:9092'
      @kafka = Kafka.new(@seed_brokers)
      @seconds_between_retries = params[:seconds_between_retries] || 0
      @num_retries = params[:num_retries] || 0
      @partitions_num = params[:partitions_num] || ENV['DEFAULT_PARTITIONS_NUM'] || 35
      @replication_factor = params[:replication_factor] || ENV['DEFAULT_REPLICATION_FACTOR'] || 3
      @topics = Array(params[:topics]) || nil
      @failures_topic = params[:failures_topic] || nil
      @group_id = params[:group_id] || 'missing_groupid'
      @start_from_beginning = params[:start_from_beginning].nil? ? true : params[:start_from_beginning]
      @consumer = @kafka.consumer(group_id: @group_id)
      @producer = Producer.new(
        client_id: @group_id,
        avro_encoding: @avro_encoding,
        logstash_logger: @use_logstash_logger,
        broker_url: @seed_brokers
      )
      trap("INT") { @consumer.stop }
    rescue => error
      log_error('Consumer Could not initialize',
                error: error.message,
                broker_url: @seed_brokers,
                backtrace: error.backtrace)
      raise 'Could not initialize'
    end

    def start_consumer
      subscribe_to_topics
      @consumer.each_message do |message|
        @consumer.mark_message_as_processed(message)
        @consumer.commit_offsets
        payload = extract_payload(message)
        handle_consume(payload, message)
      end
    rescue => error
      log_error('Consumer failed to start',
                error: error.message,
                backtrace: error.backtrace,
                topics: @topics,
                group: @group_id,
                broker_url: @seed_brokers,
                failures_topic: @failures_topic)
    end

    def subscribe_to_topics
      @topics.each do |topic|
        @consumer.subscribe(topic, start_from_beginning: @start_from_beginning)
        log_info('Consumer subscribes to topic: ' + topic, broker_url: @seed_brokers)
      end
      if @failures_topic
        @consumer.subscribe(@failures_topic, start_from_beginning: @start_from_beginning)
        log_info('Consumer subscribes to failures topic: ' + @failures_topic, broker_url: @seed_brokers)
      end
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
      key = message.key || nil
      begin
        key = key.to_s.encode('UTF-8') unless key.nil?
      rescue Encoding::UndefinedConversionError
        log_error('Key sent in invalid format')
        return
      end

      unless message.headers[YotpoKafka.retry_header_key]
        message.headers[YotpoKafka.retry_header_key] = get_init_retry_header(message.topic, key, error)
      end
      retry_hdr = update_retry_header(message.headers[YotpoKafka.retry_header_key], error)
      publish_to_retry_service(retry_hdr, message, key)
    end

    def get_init_retry_header(topic, key, error)
      {
        CurrentAttempt: @num_retries,
        NextExecTime: (Time.now.utc + @seconds_between_retries).to_datetime.rfc3339,
        Error: error.message,
        Backtrace: error.backtrace,
        MainTopic: topic,
        FailuresTopic: @failures_topic.nil? ?  get_fail_topic_name(topic) : @failures_topic,
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

    def publish_to_retry_service(retry_hdr, message, key)
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

      @producer.publish(topic, message.value, message.headers, key, false)
    end

    def handle_consume(_payload, _message)
      raise NotImplementedError
    end

    def consume_message(_message)
      raise NotImplementedError
    end

    def extract_payload(_message)
      raise NotImplementedError
    end
  end
end
