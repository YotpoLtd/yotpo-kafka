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
      @listen_to_failures = params[:listen_to_failures].nil? ? true : params[:listen_to_failures]
      @num_retries = params[:num_retries] || 0
      @topics = Array(params[:topics]) || nil
      @group_id = params[:group_id] || 'missing_groupid'
      @start_from_beginning = params[:start_from_beginning].nil? ? true : params[:start_from_beginning]
      @consumer = @kafka.consumer(group_id: @group_id)
    rescue => error
      log_error('Consumer Could not initialize',
                error: error.message,
                broker_url: @seed_brokers,
                backtrace: error.backtrace)
      raise 'Could not initialize'
    end

    def start_consumer
      log_debug('Starting consume', broker_url: @seed_brokers)
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
                broker_url: @seed_brokers)
    end

    def subscribe_to_topics
      @topics.each do |topic|
        @consumer.subscribe(topic, start_from_beginning: @start_from_beginning)
        log_info('Consumer subscribes to topic: ' + topic)
        subscribe_to_failure_topic(topic) if @listen_to_failures
      end
    end

    def subscribe_to_failure_topic(topic)
      failure_topic = get_fail_topic_name(topic)
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

    def get_fail_topic_name(main_topic)
      main_topic.tr('.', '_')
      group = @group_id.tr('.', '_').gsub('::', '_')
      main_topic + '.' + group + YotpoKafka.failures_topic_suffix
    end

    def handle_consume(_payload, _message)
      raise NotImplementedError
    end

    def extract_payload(_message)
      raise NotImplementedError
    end

    def consume_message(_message)
      raise NotImplementedError
    end
  end
end
