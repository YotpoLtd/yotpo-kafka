require 'phobos'
require 'kafka'
require 'date'
require 'securerandom'
require 'ylogger'
require 'json'

module YotpoKafka
  class Producer
    include ::Phobos::Producer
    extend Ylogger

    def initialize(context = {})
      params = HashWithIndifferentAccess.new(context)
      @kafka_broker_url = params[:kafka_broker_url]
      @gap_between_retries = params[:gap_between_retries] || 0
      @num_retries = params[:num_retries] || 0
      @client_id = params[:client_id] || 'yotpo-kafka'
      @active_job = params[:active_job] || nil
      @red_cross = params[:red_cross] || nil
      @logstash_logger = params[:logstash_logger] || true
      config
    rescue => error
      log_error("Producer failed to initialize", {error: error})
      raise 'Producer failed to initialize'
    end

    def config
      YotpoKafka::RedCrossKafka.config_red_cross(@red_cross) unless @red_cross.nil?
      YotpoKafka::YLoggerKafka.config(@logstash_logger)
      YotpoKafka::ActiveJobs.config(@active_job)
      YotpoKafka::ProducerConfig.configure(@kafka_broker_url, @client_id)
    end


    def publish(topic, message, key = nil, msg_id = nil)
      if message['kafka_header'].nil?
        message['kafka_header'] = {timestamp: DateTime.now,
                                   msg_id: msg_id || SecureRandom.uuid,
                                   kafka_broker_url: @kafka_broker_url}
      end
      topics = Array(topic)
      topics.each do |topic|
        publish_messages([{ topic: topic, payload: message.to_json, key: key }])
      end
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
      RedCross.monitor_track(event: 'messagePublished', properties: { success: true }) unless @red_cross.nil?
    rescue => error
      log_error("Publish failed", {error: error})
      RedCross.monitor_track(event: 'messagePublished', properties: { success: false }) unless @red_cross.nil?
      messages.each do |message|
        params = HashWithIndifferentAccess.new(message)
        if @active_job
          if @num_retries > 0
            enqueue(params[:payload],
                    params[:topic],
                    params[:key],
                    params[:msg_id],
                    error)
          elsif @num_retries == 0
            enqueue(params[:payload],
                    "#{params[:topic]}_fatal",
                    params[:key],
                    params[:msg_id],
                    error)
          end
        end
      end
    end

    def enqueue(payload, topic, key, msg_id, error)
      params = {'gap_between_retries' => @gap_between_retries,
                'kafka_broker_url' => @kafka_broker_url,
                'num_retries' => @num_retries - 1,
                'client_id' => @client_id,
                'active_job' => @active_job,
                'red_cross' => @red_cross,
                'logstash_logger' => @logstash_logger,
                'topic'  => topic,
                'payload' => payload,
                'key' => key,
                'msg_id' => msg_id,
                'exception_message' => error}
      ProducerWorker.set(wait: @gap_between_retries.second).perform_later(params.to_json)
    rescue => error
      log_error("Enqueue failed", {error: error})
    end
  end
end
