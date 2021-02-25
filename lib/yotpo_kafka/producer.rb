module YotpoKafka
  class Producer < YotpoKafka::BaseProducer

    def publish(topic, payload, headers = {}, key = nil, to_json = true)
      begin
        payload = payload.to_json if to_json
      rescue Encoding::UndefinedConversionError
        log_error('Failed to convert msg to json')
      end
      handle_produce(payload, key, topic, headers)
    rescue => error
      handle_produce_failures(topic, error)
    end

    def publish_multiple(topic, payloads, headers = {}, key = nil, to_json = true)
      payloads.each do |payload|
        publish(topic, payload, headers, key, to_json)
      end
    rescue => error
      log_error('Publish multi messages failed',
                exception: error.message)
      raise error
    end

    def handle_produce_failures(topic, error)
      log_error('Single publish failed',
                broker_url: @seed_brokers,
                topic: topic,
                error: error.message)
      raise error
    end
  end
end
