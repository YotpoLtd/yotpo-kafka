module YotpoKafka
  class Consumer < YotpoKafka::BaseConsumer

    def initialize(params = {})
      super
    end

    def handle_consume(payload, message)
      consume_message(payload)
    rescue => error
      handle_consume_error(message, error)
    end

    def extract_payload(_message)
      message.value
    end
  end
end
