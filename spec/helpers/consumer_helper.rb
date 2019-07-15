module Helpers
  class ConsumerHandler < YotpoKafka::Consumer
    def consume_message(_message)
      puts 'message consumed'
    end
  end

  class ConsumerHandlerWithError < YotpoKafka::Consumer
    def consume_message(_message)
      raise
    end
  end
end
