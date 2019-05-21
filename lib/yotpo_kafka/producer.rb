require 'kafka'
require 'date'
require 'securerandom'
require 'ylogger'
require 'json'

module YotpoKafka
  class Producer
    extend Ylogger

    def initialize(params = {})
      YotpoKafka::YLoggerKafka.config(true)
      set_log_tag(:yotpo_ruby_kafka)
      YotpoKafka.kafka = Kafka.new(YotpoKafka.seed_brokers)
      @producer = YotpoKafka.kafka.producer
      @red_cross = params[:red_cross] || false
    rescue => error
      log_error('Producer failed to initialize',
                exception: error.message,
                broker_url: YotpoKafka.seed_brokers)
      raise 'Producer failed to initialize'
    end

    def publish(topic, value, kafka_v2_headers = {}, key = nil, to_json = true)
      log_info('Publishing message',
               topic: topic, message: value, headers: kafka_v2_headers, key: key, broker_url: YotpoKafka.seed_brokers)
      value = value.to_json if to_json
      if YotpoKafka.kafka_v2
        @producer.produce(value, key: key, headers: kafka_v2_headers, topic: topic)
      else
        @producer.produce(value, key: key, topic: topic)
      end
      @producer.deliver_messages
    rescue => error
      log_error('Single publish failed',
                broker_url: YotpoKafka.seed_brokers,
                message: value,
                headers: kafka_v2_headers,
                topic: topic,
                error: error.message)
    end

    def publish_multiple(topic, payloads, kafka_v2_headers = {}, key = nil, to_json = true)
      log_info('Publishing multiple messages',
               topic: topic, message: value, headers: kafka_v2_headers, key: key, broker_url: YotpoKafka.seed_brokers)
      payloads.each do |payload|
        publish(topic, payload, kafka_v2_headers, key, to_json)
      end
      RedCross.monitor_track(event: 'messagePublished', properties: { success: true }) if @red_cross
    rescue => error
      log_error('Publish multi messages failed',
                exception: error.message)
      RedCross.monitor_track(event: 'messagePublished', properties: { success: false }) if @red_cross
    end
  end
end
