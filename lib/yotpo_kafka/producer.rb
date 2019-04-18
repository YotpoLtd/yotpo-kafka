require 'kafka'
require 'date'
require 'securerandom'
require 'ylogger'
require 'json'

module YotpoKafka
  class Producer
    extend Ylogger

    def initialize(params = {})
      @producer = YotpoKafka.kafka.producer
      @red_cross = params[:red_cross] || false
      set_log_tag(:yotpo_ruby_kafka)
      YotpoKafka::YLoggerKafka.config(true)
      log_info("Producer yotpo-ruby-kafka 1.0.9 broker address " + YotpoKafka.seed_brokers)
    rescue StandardError => e
      log_error('Producer failed to initialize',
                exception: e.message,
                broker_url: YotpoKafka.seed_brokers)
      raise 'Producer failed to initialize'
    end

    def publish(topic, value, headers = {}, key = nil)
      log_info('Publishing message to topic ' + topic,
               message: value, headers: headers, key: key, broker_url: YotpoKafka.seed_brokers)
      if headers.empty?
        @producer.produce(value.to_json, key: key, topic: topic)
      else
        @producer.produce(value, key: key, headers: headers, topic: topic)
      end
      @producer.deliver_messages
    rescue StandardError => e
      log_error('Single publish to topic ' + topic + ' failed with error: ' + e.message,
                broker_url: YotpoKafka.seed_brokers)
    end

    def publish_multiple(topic, payloads, headers = {}, key = nil)
      log_info('Publish multiply messages to topic ' + topic,
               message: value, headers: headers, key: key, broker_url: YotpoKafka.seed_brokers)
      payloads.each do |payload|
        publish(topic, payload, headers, key)
      end
      log_info('Messages published successfully')
      RedCross.monitor_track(event: 'messagePublished', properties: { success: true }) if @red_cross
    rescue StandardError => e
      log_error('Publish multi messages failed',
                exception: e.message)
      RedCross.monitor_track(event: 'messagePublished', properties: { success: false }) if @red_cross
    end
  end
end
