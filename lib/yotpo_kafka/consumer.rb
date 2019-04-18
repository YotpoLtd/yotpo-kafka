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
      trap("TERM") { @consumer.stop }
      @producer = Producer.new(
        client_id: @group_id,
        logstash_logger: true
      )
      log_info("Consumer yotpo-ruby-kafka 1.0.11 broker address " + YotpoKafka.seed_brokers)
    rescue StandardError => e
      log_error('Consumer Could not initialize',
                exception: e.message,
                broker_url: YotpoKafka.seed_brokers)
      raise 'Could not initialize'
    end

    def start_consumer
      log_info('Starting consume',
                broker_url: YotpoKafka.seed_brokers)
      subscribe_to_topics
      @consumer.each_message do |message|
        @consumer.mark_message_as_processed(message)
        handle_consume(message)
      end
    rescue StandardError => e
      log_error('Consumer failed to start: ' + e.message,
                exception: e.message,
                broker_url: YotpoKafka.seed_brokers)
    end

    def handle_consume(message)
      if YotpoKafka.include_headers
        consume_with_headers(message)
      else
        handle_consume_without_headers(message)
      end
    end

    def consume_with_headers(message)
      log_info('Handle consume',
               payload: message.value, topic: message.topic, broker_url: YotpoKafka.seed_brokers)
      consume_message(message.value)
      log_info('Message consumed',
               topic: message.topic, broker_url: YotpoKafka.seed_brokers)
      RedCross.monitor_track(event: 'messageConsumed', properties: { success: true }) if @red_cross
    rescue StandardError => e
      RedCross.monitor_track(event: 'messageConsumed', properties: { success: false }) if @red_cross
      log_error('Consume error: ' + e.message, topic: message.topic)
      handle_error_with_headers(message, e)
    end

    def handle_consume_without_headers(message)
      log_info('Handle consume',
               payload: message.value, topic: message.topic, broker_url: YotpoKafka.seed_brokers)
      parsed_payload = JSON.parse(message.value)
      unless parsed_payload.is_a?(Hash)
        raise JSON::ParserError.new('Parse didnt finish correctly')
      end
      consume_message(parsed_payload)
      log_info('Message consumed', topic: message.topic)
      RedCross.monitor_track(event: 'messageConsumed', properties: { success: true }) if @red_cross
    rescue JSON::ParserError => parseError
      log_error('Failed to parse payload to json: ' + message.value, topic: message.topic)
      RedCross.monitor_track(event: 'messageConsumed', properties: { success: false }) if @red_cross
      log_error('Consume parse error - no retry: ' + parseError.to_s, topic: message.topic)
    rescue StandardError => error
      RedCross.monitor_track(event: 'messageConsumed', properties: { success: false }) if @red_cross
      log_error('Consume error: ' + error.message, topic: message.topic)
      handle_error_without_headers(parsed_payload, message.topic, message.key, error)
    end

    def subscribe_to_topics
      @topics.each do |t|
        @consumer.subscribe(t)
        log_info('Consume subscribes to topic: ' + t)
        next unless @listen_to_failures

        failure_topic = build_fail_topic(t)
        begin
          YotpoKafka.kafka.create_topic(failure_topic)
          log_info('YotpoKafka created new topic: ' + failure_topic)
        rescue Kafka::TopicAlreadyExists
        end

        @consumer.subscribe(failure_topic)
        log_info('Consume subscribes to topic: ' + failure_topic)
      end
    end

    def build_fail_topic(main_topic)
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
          FailuresTopic: build_fail_topic(topic),
          delayIntervalSec: @seconds_between_retries
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
        log_info('Message was not consumed - wait for retry', topic: topic)
        if @seconds_between_retries.zero?
          @producer.publish(parsed_hdr['FailuresTopic'], payload, {}, key)
        else
          @producer.publish(YotpoKafka.retry_topic, payload, {}, key)
        end
      else
        retry_hdr[:NextExecTime] = Time.now.utc.to_datetime.rfc3339
        payload[YotpoKafka.retry_header_key] = retry_hdr.to_json
        log_info('Message was not consumed - sent to fatal', topic: topic)
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
          FailuresTopic: build_fail_topic(message.topic),
          delayIntervalSec: @seconds_between_retries
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
        log_info('Message was not consumed - wait for retry', topic: message.topic)
        if @seconds_between_retries.zero?
          @producer.publish(parsed_hdr['FailuresTopic'], message.value, message.headers, message.key)
        else
          @producer.publish(YotpoKafka.retry_topic, message.value, message.headers, message.key)
        end
      else
        retry_hdr[:NextExecTime] = Time.now.utc.to_datetime.rfc3339
        message.headers[YotpoKafka.retry_header_key] = retry_hdr.to_json
        log_info('Message was not consumed - sent to fatal', topic: message.topic)
        @producer.publish(YotpoKafka.fatal_topic, message.value, message.headers, message.key)
      end
    end

    def consume_message(_message)
      raise NotImplementedError
    end
  end
end
