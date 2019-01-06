module YotpoKafka
  class ProducerWorker < ActiveJob::Base
    @queue = :KafkaWorkerJobs
    def perform(context)
      params = JSON.parse(context)
      producer = YotpoKafka::Producer.new('gap_between_retries' => params['gap_between_retries'],
                                          'kafka_broker_url' => params['kafka_broker_url'],
                                          'num_retries' => (params['num_retries'].to_i if params['num_retries']),
                                          'client_id' => params['client_id'],
                                          'active_job' => (params['active_job'].to_sym if params['active_job']),
                                          'red_cross' => (params['red_cross'].to_sym if params['red_cross']),
                                          'logstash_logger'=>( params['logstash_logger'].to_bool if params['logstash_logger']))
      producer.publish(params['topic'], params['payload'], params['key'], params['msg_id'])
    rescue => error
      raise "an error occurred, error: #{error}"
    end
  end
end