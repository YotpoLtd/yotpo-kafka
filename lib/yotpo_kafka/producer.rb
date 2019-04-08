require 'kafka'
require 'date'
require 'securerandom'
require 'ylogger'
require 'json'

module YotpoKafka
  class Producer
    extend Ylogger

    def initialize(params)
      @producer = YotpoKafka.kafka.producer
      @client_id = params[:client_id] || 'missing_groupid'
      @red_cross = params[:red_cross] || false
      @logstash_logger = params[:logstash_logger] || true
    rescue StandardError => e
      log_error('Producer failed to initialize',
                exception: e.message,
                log_tag: 'yotpo-ruby-kafka')
      raise 'Producer failed to initialize'
    end

    def publish(topic, value, headers = {}, key = nil)
      @producer.produce(value, key: key, headers: headers, topic: topic)
      @producer.deliver_messages
    end

    def publish_multiple(topic, payloads, headers = {}, key = nil)
      payloads.each do |payload|
        publish(topic, payload, headers, key)
      end
      log_info('Messages published successfully', log_tag: 'yotpo-ruby-kafka')
      RedCross.monitor_track(event: 'messagePublished', properties: { success: true }) if @red_cross
    rescue StandardError => error
      log_error('Publish failed',
                exception: error.message,
                log_tag: 'yotpo-ruby-kafka')
      RedCross.monitor_track(event: 'messagePublished', properties: { success: false }) if @red_cross
    end
  end
end
