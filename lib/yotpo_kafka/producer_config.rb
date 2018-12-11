module YotpoKafka
  class ProducerConfig
    def self.configure(kafka_broker_url, client_id)
      Phobos.configure(
        consumer: {},
        backoff: { min_ms: 1000, max_ms: 60000 },
        logger: { ruby_kafka: { level: :info } },
        kafka: { client_id: client_id, seed_brokers: kafka_broker_url.split(',') },
        producer: {}
      )
    end
  end
end
