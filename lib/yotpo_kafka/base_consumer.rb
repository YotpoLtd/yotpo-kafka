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
      @partitions_num = params[:partitions_num] || ENV['DEFAULT_PARTITIONS_NUM'] || 35
      @replication_factor = params[:replication_factor] || ENV['DEFAULT_REPLICATION_FACTOR'] || 3
      @topics = Array(params[:topics]) || nil
      @failures_topic = params[:failures_topic] || nil
      @group_id = params[:group_id] || 'missing_groupid'
      @start_from_beginning = params[:start_from_beginning].nil? ? true : params[:start_from_beginning]
      @consumer = @kafka.consumer(group_id: @group_id)
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
        subscribe_to_failure_topic(topic) if @listen_to_failures && @failures_topic.nil?
      end
      if @failures_topic
        @consumer.subscribe(@failures_topic, start_from_beginning: @start_from_beginning)
        log_info('Consumer subscribes to failures topic: ' + @failures_topic, broker_url: @seed_brokers)
      end
    end

    def generate_failure_topic(topic)
      begin
        @kafka.create_topic(topic,
                            num_partitions: @partitions_num.to_i,
                            replication_factor: @replication_factor.to_i)
        log_error('Deprecated. Created new topic',
                  topic: topic,
                  partitions_num: @partitions_num,
                  replication_factor: @replication_factor)
      rescue Kafka::TopicAlreadyExists
        nil
      end
    end

    def subscribe_to_failure_topic(topic)
      failure_topic = get_fail_topic_name(topic)
      generate_failure_topic(failure_topic)

      @consumer.subscribe(failure_topic)
      log_error('Deprecated. Consumer subscribes to failure topic',
                failure_topic: failure_topic, broker_url: @seed_brokers)
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
