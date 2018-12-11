require 'phobos'
require 'kafka'
require 'date'
require 'securerandom'

module YotpoKafka
  class Producer
    include ::Phobos::Producer
    ALL_PRODUCE_FAILURES_FROM_ALL_TOPICS = 'all_produce_failures_from_all_topics'

    def initialize(params = {})
      @gap_between_retries = params[:gap_between_retries] || 0
      @kafka_broker_url = params[:kafka_broker_url]
      @num_retries = params[:num_retries] || 0
      @logger = params[:logger] || nil
      @client_id = params[:client_id] || 'yotpo-kafka'
      YotpoKafka::ProducerConfig.configure(@kafka_broker_url, @client_id)
    rescue => error
      if @logger
        @logger.error "The following error occurred when configuring: #{error}"
      end
    end

    def publish(topic, message, key = nil, msg_id = nil)
      payload = message
      if message['header'].nil?
        payload = { header: {timestamp: DateTime.now,
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
    end

    def publish_messages(messages)
      producer.publish_list(messages)
      YotpoKafka::Producer.producer.kafka_client.close
    rescue => error
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
                  ALL_PRODUCE_FAILURES_FROM_ALL_TOPICS,
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
      if @logger
        @logger.error "enqueue with msg_id #{msg_id} failed due to #{error}"
      end
    end
  end
end
