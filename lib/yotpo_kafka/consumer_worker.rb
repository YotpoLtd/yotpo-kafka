class ConsumerWorker
  @queue = :KafkaWorkerJobs
  class << self
    def perform(context)
      params = HashWithIndifferentAccess.new(context)
      producer = YotpoKafka::Producer.new({kafka_broker_url: params[:kafka_broker_url],
                                           num_of_retries: 2})
      payload = JSON.parse(Base64.decode64(params[:base64_payload]))
      producer.publish(params[:topic], payload)
    rescue => error
      raise "An error occurred, error: #{error}"
    end
  end
end