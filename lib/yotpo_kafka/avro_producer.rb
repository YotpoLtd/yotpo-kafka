module YotpoKafka
  class AvroProducer < YotpoKafka::BaseProducer

    def initialize(params = {})
      require 'avro_turf/messaging'
      @registry_url = params[:registry_url] || ENV['REGISTRY_URL'] || '127.0.0.1:8081'
      @avro_messaging = AvroTurf::Messaging.new(registry_url: @registry_url)
      super
    end

    def publish(topic, payload, subject, headers = {}, key = nil, subject_version = 'latest', validate: false)
      payload_avro_encoded = @avro_messaging.encode(payload, subject: subject, version: subject_version, validate: validate)
      handle_produce(payload_avro_encoded, key, topic, headers)
    rescue => error
      handle_produce_failures(topic, error)
    end

    def publish_multiple(topic, payloads, subject, headers = {}, key = nil, subject_version = 'latest', validate: false)
      payloads.each do |payload|
        publish(topic, payload, headers, key)
      end
    rescue => error
      log_error('Publish multi messages failed',
                exception: error.message)
      raise error
    end

    def handle_produce_failures(topic, error)
      log_error('Single publish failed',
                broker_url: @seed_brokers,
                registry_url: @registry_url,
                topic: topic,
                error: error.message)
      raise error
    end
  end
end
