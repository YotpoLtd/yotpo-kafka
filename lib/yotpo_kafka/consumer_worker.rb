module YotpoKafka
  class ConsumerWorker < ActiveJob::Base
    @queue = :KafkaWorkerJobs
    def perform(context)
      params = JSON.parse(context)
      producer = YotpoKafka::Producer.new('kafka_broker_url' => params['kafka_broker_url'],
                                           'num_retries' => -1,
                                           'client_id' => params['client_id'],
                                           'active_job' => (params['active_job'].to_sym if params['active_job']),
                                           'red_cross' => (params['red_cross'].to_sym if params['red_cross']),
                                           'logstash_logger'=> params['logstash_logger'])
      producer.publish(params['topic'], params['payload'])
    rescue => error
      raise "an error occurred, error: #{error}"
    end
  end
end
