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
      log_error('Producer failed to initialize',
                exception: error.message,
                broker_url: @seed_brokers)
      raise error
    end

    def publish(topic, payload, headers = {}, key = nil, to_json = true)
      log_debug('Publishing message',
                topic: topic,
                headers: headers,
                key: key,
                broker_url: @seed_brokers)
      begin
        payload = payload.to_json if to_json
      rescue Encoding::UndefinedConversionError
        log_error('Failed to convert msg to json')
      end
      payload = @avro.encode(payload) if @avro
      handle_produce(payload, key, topic, headers)
      log_debug('Publish done')
    rescue => error
      handle_produce_failures(topic, error)
    end

    def publish_multiple(topic, payloads, headers = {}, key = nil, to_json = true)
      log_debug('Publishing multiple messages',
                topic: topic,
                message: value,
                headers: headers,
                key: key,
                broker_url: @seed_brokers)
      payloads.each do |payload|
        publish(topic, payload, headers, key, to_json)
      end
    rescue => error
      log_error('Publish multi messages failed',
                exception: error.message)
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
      begin
        RedCross.track(event: 'produce_failure', properties: { topic: topic })
      rescue => e
        log_error('Failed to report failure to influx',
                  broker_url: @seed_brokers,
                  topic: topic,
                  error: e.message)
      end
      raise error
    end

    def set_avro_registry(registry_url)
      require 'avro_turf/messaging'
      @avro = AvroTurf::Messaging.new(registry_url: registry_url)
    end
  end
end
