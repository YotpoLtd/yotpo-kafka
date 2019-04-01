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
      @client_id = params[:client_id] || 'yotpo-kafka'
      @red_cross = params[:red_cross] || nil
      @logstash_logger = params[:logstash_logger] || true
      config
    rescue => error
      log_error('Producer failed to initialize', error: error)
      raise 'Producer failed to initialize'
    end

    def config
      YotpoKafka::RedCrossKafka.config_red_cross(@red_cross) unless @red_cross.nil?
      YotpoKafka::YLoggerKafka.config(@logstash_logger)
    end

    def publish(value, headers, topic, key = nil)
      @producer.produce(value, key: key, headers: headers, topic: topic)
      @producer.deliver_messages
    end

    def publish_multiple(messages)
      messages.each do |message|
        publish(message.topic, message.payload, message.key)
      end
      log_info('Messages published successfully')
      RedCross.monitor_track(event: 'messagePublished', properties: { success: true }) unless @red_cross.nil?
    rescue => error
      log_error('Publish failed', error: error)
      RedCross.monitor_track(event: 'messagePublished', properties: { success: false }) unless @red_cross.nil?
    end
  end
end
