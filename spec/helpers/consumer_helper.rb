module Helpers
  class ConsumerHandler < YotpoKafka::Consumer
    attr_accessor :num_retries
    def consume_message(_message)
      puts 'message consumed'
    end
  end
end
