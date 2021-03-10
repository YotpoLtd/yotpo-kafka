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
    rescue => error
      log_error('Producer failed to initialize',
                exception: error.message,
                broker_url: @seed_brokers)
      raise error
    end

    def publish(topic, payload, headers = {}, key = nil, to_json = true)
      begin
        payload = payload.to_json if to_json
      rescue Encoding::UndefinedConversionError
        log_error('Failed to convert msg to json')
      end
      handle_produce(payload, key, topic, headers)
    rescue => error
      handle_produce_failures(topic, error)
    end

    def publish_multiple(topic, payloads, headers = {}, key = nil, to_json = true)
      payloads.each do |payload|
        publish(topic, payload, headers, key, to_json)
      end
    rescue => error
      log_error('Publish multi messages failed',
                exception: error.message)
      raise error
    end

    def handle_produce(payload, key, topic, headers)
      @producer.produce(payload, key: key, headers: headers, topic: topic)
      @producer.deliver_messages
    end

    def handle_produce_failures(topic, error)
      log_error('Single publish failed',
                broker_url: @seed_brokers,
                topic: topic,
                error: error.message)
      raise error
    end
  end
end
