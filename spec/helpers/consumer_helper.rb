module Helpers
  class ConsumerHandler < YotpoKafka::Consumer
    attr_accessor :num_retries

    def initialize(_context = {})
      params = { 'gap_between_retries' => 2,
                 'num_retries' => 2,
                 'logstash_logger' => true }
      super(params)
    end

    def consume_message(_message)
      puts 'message consumed'
    end
  end
end
