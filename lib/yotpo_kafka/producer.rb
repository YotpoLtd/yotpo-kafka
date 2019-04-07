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
      log_error('Producer failed to initialize',
                exception: error.message,
                log_tag: 'yotpo-ruby-kafka')
      raise 'Producer failed to initialize'
    end

    def config
      YotpoKafka::RedCrossKafka.config_red_cross(@red_cross) unless @red_cross.nil?
      YotpoKafka::YLoggerKafka.config(@logstash_logger)
    end

    def publish(topic, value, headers=nil, key = nil)
      @producer.produce(value, key: key, headers: headers, topic: topic)
      @producer.deliver_messages
    end

    def publish_multiple(topic, messages)
      messages.each do |message|
        publish(topic, message.payload, message.headers, message.key)
      end
      log_info('Messages published successfully', log_tag: 'yotpo-ruby-kafka')
      RedCross.monitor_track(event: 'messagePublished', properties: { success: true }) unless @red_cross.nil?
    rescue => error
      log_error('Publish failed',
                exception: error.message,
                log_tag: 'yotpo-ruby-kafka')
      RedCross.monitor_track(event: 'messagePublished', properties: { success: false }) unless @red_cross.nil?
    end
  end
end
