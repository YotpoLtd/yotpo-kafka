module Helpers
  class DummyMsg
    attr_reader :value, :key, :headers, :topic
    def initialize(value: 'value', key: 'key', headers: {}, topic: 'topic')
      @value = value
      @key = key
      @headers = headers
      @topic = topic
      @bytesize = key.to_s.bytesize + value.to_s.bytesize
    end
  end


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

  class AvroConsumerHandlerWithError < YotpoKafka::AvroConsumer
    def consume_message(_message)
      raise
    end
  end
end
