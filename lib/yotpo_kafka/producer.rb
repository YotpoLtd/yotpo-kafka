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
    rescue StandardError => e
      log_error('YotpoKafka producer failed to initialize',
                exception: e.message,
                broker_url: YotpoKafka.seed_brokers)
      raise 'Producer failed to initialize'
    end

    def publish(topic, value, headers = {}, key = nil)
      log_info('YotpoKafka publishing message',
               topic: topic, message: value, headers: headers, key: key, broker_url: YotpoKafka.seed_brokers)
      if headers.empty?
        @producer.produce(value.to_json, key: key, topic: topic)
      else
        @producer.produce(value, key: key, headers: headers, topic: topic)
      end
      @producer.deliver_messages
    rescue StandardError => e
      log_error('YotpoKafka single publish failed',
                broker_url: YotpoKafka.seed_brokers,
                topic: topic,
                error: e.message)
    end

    def publish_multiple(topic, payloads, headers = {}, key = nil)
      log_info('YotpoKafka publishing multiply messages',
               topic: topic, message: value, headers: headers, key: key, broker_url: YotpoKafka.seed_brokers)
      payloads.each do |payload|
        publish(topic, payload, headers, key)
      end
      RedCross.monitor_track(event: 'messagePublished', properties: { success: true }) if @red_cross
    rescue StandardError => e
      log_error('Publish multi messages failed',
                exception: e.message)
      RedCross.monitor_track(event: 'messagePublished', properties: { success: false }) if @red_cross
    end
  end
end
