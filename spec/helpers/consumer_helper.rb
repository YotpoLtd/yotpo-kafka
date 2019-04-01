module Helpers
  class ConsumerHandler < YotpoKafka::Consumer
    attr_accessor :num_retries

    def initialize(context = {})
      params = { 'gap_between_retries' => 2,
                 'num_retries' =>  2,
                 'logstash_logger' => true}
      super(params)
    end

    def consume_message(_message)
      puts 'message consumed'
    rescue => e
      raise
    end
   end
end
