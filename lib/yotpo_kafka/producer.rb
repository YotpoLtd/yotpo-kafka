require 'phobos'
require 'kafka'
require 'date'
require 'securerandom'
require 'ylogger'

module YotpoKafka
  class Producer
    include ::Phobos::Producer
    extend Ylogger

    def initialize(params = {})
      @gap_between_retries = params[:gap_between_retries] || 0
      @kafka_broker_url = params[:kafka_broker_url]
      @num_retries = params[:num_retries] || 0
      @logger = params[:logger] || nil
      @client_id = params[:client_id] || 'yotpo-kafka'
      @use_red_cross = params[:red_cross] || nil
      config(params)
    rescue => error
      log_error("Producer failed to initialize", {error: error})
    end

    def config(params)
      YotpoKafka::RedCrossKafka.config_red_cross(params[:red_cross]) unless params[:red_cross].nil?
      YotpoKafka::YLoggerKafka.config(params[:logstash_logger] || false)
      YotpoKafka::ProducerConfig.configure(@kafka_broker_url, @client_id)
    end


    def publish(topic, message, key = nil, msg_id = nil)
      payload = message
      if message['kafka_header'].nil?
        payload = { kafka_header: {timestamp: DateTime.now,
                             msg_id: msg_id || SecureRandom.uuid,
                             kafka_broker_url: @kafka_broker_url},
                    message: message}
      end
      publish_messages([{ topic: topic, payload: payload.to_json, key: key }])
    end

    def publish_multiple(messages)
      messages.each do |message|
        publish(message[:topic], message[:message], message[:key], message[:msg_id])
      end
      log_info("Messages sent successfully")
    end

    def publish_messages(messages)
      producer.publish_list(messages)
      YotpoKafka::Producer.producer.kafka_client.close
      RedCross.monitor_track(event: 'messagePublished', properties: { success: true }) unless @use_red_cross.nil?
    rescue => error
      log_error("Enqueue failed", {error: error})
      RedCross.monitor_track(event: 'messagePublished', properties: { success: false }) unless @use_red_cross.nil?
      messages.each do |message|
        params = HashWithIndifferentAccess.new(message)
        if @logger
          @logger.error "message with msg_id #{params[:msg_id]}
                         failed to publish due to #{error}"
        end
        if @num_of_retries > 0
          enqueue(params[:payload],
                  params[:topic],
                  params[:key],
                  parmas[:msg_id],
                  error)
        elsif @num_of_retries == 0
          enqueue(params[:payload],
                  "#{params[:topic]}_fatal",
                  params[:key],
                  parmas[:msg_id],
                  error)
        end
      end
    end

    def enqueue(payload, topic, key, msg_id, error)
      Resque.enqueue_in(@gap_between_retries,
                        ProducerWorker,
                        topic: topic,
                        base64_payload: Base64.encode64(payload),
                        kafka_broker_url: @kafka_broker_url,
                        key: key,
                        msg_id: msg_id,
                        num_of_retries: @num_retries - 1,
                        exception_message: error)
    rescue => error
      log_error("Enqueue failed", {error: error})
    end
  end
end
