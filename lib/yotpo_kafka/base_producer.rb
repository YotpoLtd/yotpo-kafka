require 'kafka'
require 'date'
require 'securerandom'
require 'ylogger'
require 'json'
require 'rest-client'

module YotpoKafka
  class BaseProducer
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

    def handle_produce(payload, key, topic, headers)
      @producer.produce(payload, key: key, headers: headers, topic: topic)
      @producer.deliver_messages
    end

    def publish(topic, payload, headers = {}, key = nil)
      raise NotImplementedError
    end

    def publish_multiple(topic, payloads, headers = {}, key = nil)
      raise NotImplementedError
    end
  end
end
