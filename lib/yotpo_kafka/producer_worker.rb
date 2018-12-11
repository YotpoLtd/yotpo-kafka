class ProducerWorker
  @queue = :KafkaWorkerJobs
  class << self
    def perform(context)
      params = HashWithIndifferentAccess.new(context)
      message = JSON.parse(Base64.decode64(params[:base64_payload]))
      producer = YotpoKafka::Producer.new(kafka_broker_url: params[:kafka_broker_url], num_of_retries: params[:num_of_retries])
      producer.publish(params[:topic], message, params[:key], params[:msg_id])
    end
  end
end