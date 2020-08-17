require 'kafka'
require 'date'
require 'securerandom'
require 'ylogger'
require 'json'
require 'rest-client'

module YotpoKafka
  class Producer
    extend Ylogger

    def initialize(params = {})
      compression = params[:compression] ? :gzip : nil
      use_logstash_logger = params[:logstash_logger] != false
      YotpoKafka::YLoggerKafka.config(use_logstash_logger)
      set_log_tag(:yotpo_ruby_kafka)
      @seed_brokers = params[:broker_url] || ENV['BROKER_URL'] || '127.0.0.1:9092'
      @kafka = Kafka.new(@seed_brokers)
      @producer = @kafka.producer(compression_codec: compression)
      @avro_encoding = params[:avro_encoding] || false
    rescue => error
      error_msg = 'Producer failed to initialize'
      log_error(error_msg,
                exception: error.message,
                broker_url: @seed_brokers)
      handle_produce_failures('', error_msg)
    end

    def publish(topic, payload, headers = {}, key = nil, to_json = true)
      unsafe_publish(topic, payload, headers, key, to_json)
    rescue => error
      handle_produce_failures(topic, error.message)
    end

    def async_publish_with_retry(topic, value, headers = {}, key = nil,
                                 immediate_retry_count = 3, interval_between_retry = 2, to_json = true)
      backtrace_keeper = caller
      backtrace_keeper = backtrace_keeper[0..5] if backtrace_keeper.length > 6
      is_published = false
      error_msg = ''
      thread = Thread.new {
        (1..immediate_retry_count).each do |try_num|
          begin
            unsafe_publish(topic, value, headers, key, to_json)
            is_published = true
            break
          rescue => error
            log_error('Async publish failed',
                      attempt: try_num.to_s,
                      topic: topic,
                      broker_url: @seed_brokers,
                      error: error.message,
                      backtrace: backtrace_keeper)
            sleep(interval_between_retry)
            error_msg = error.message
          end
        end
        handle_produce_failures(topic, error_msg) unless is_published
      }
      thread
    end

    def publish_multiple(topic, payloads, kafka_v2_headers = {}, key = nil, to_json = true)
      log_debug('Publishing multiple messages',
                topic: topic,
                message: value,
                headers: kafka_v2_headers,
                key: key,
                broker_url: @seed_brokers)
      payloads.each do |payload|
        publish(topic, payload, kafka_v2_headers, key, to_json)
      end
    rescue => error
      log_error('Publish multi messages failed',
                exception: error.message)
    end

    def unsafe_publish(topic, payload, kafka_v2_headers = {}, key = nil, to_json = true)
      log_debug('Publishing message',
                topic: topic,
                headers: kafka_v2_headers,
                key: key,
                broker_url: @seed_brokers)
      begin
        payload = payload.to_json if to_json
      rescue Encoding::UndefinedConversionError
        log_error('Failed to convert msg to json')
      end
      payload = @avro.encode(payload) if @avro
      handle_produce(payload, key, topic, kafka_v2_headers)
      log_debug('Publish done')
    rescue => error
      log_error('Single publish failed',
                broker_url: @seed_brokers,
                topic: topic,
                error: error.message)
      raise error
    end

    def publish(topic, payload, kafka_v2_headers = {}, key = nil, to_json = true)
      unsafe_publish(topic, payload, kafka_v2_headers, key, to_json)
    rescue => error
      handle_produce_failures(topic, error)
    end

    def handle_produce(payload, key, topic, kafka_v2_headers)
      @producer.produce(payload, key: key, headers: kafka_v2_headers, topic: topic)
      @producer.deliver_messages
    end

    def handle_produce_failures(topic, error_msg)
      log_error('Produce failure',
                error: error_msg,
                topic: topic)
      RedCross.track(event: 'produce_failure', properties: { topic: topic })
      raise Exception 'Produce failure'
    end

  end
end
