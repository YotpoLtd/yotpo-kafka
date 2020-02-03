require 'avro_turf/messaging'

module YotpoKafka
  class AvroConsumer < YotpoKafka::BaseConsumer
    DEFAULT_SCHEMA_REGISTRY_URL = 'https://schema-registry.us-east-1.yotpo.xyz'.freeze

    def initialize(params = {})
      @avro_messaging = AvroTurf::Messaging.new(registry_url: ENV['REGISTRY_URL'] || DEFAULT_SCHEMA_REGISTRY_URL)
      super
    end

    def extract_payload(message)
      payload = message.value
      @avro_messaging.decode(payload).to_json
    end

    def handle_consume(payload, message)
      print_payload = get_printed_payload(payload)
      log_debug('Start handling consume',
                payload: print_payload, topic: message.topic, broker_url: @seed_brokers)

      payload = JSON.parse(payload) if @json_parse
      consume_message(payload)
    rescue => error
      log_error('avro consumer failed in service',
                message: error.message, topic: message.topic, payload: print_payload, backtrace: error.backtrace)
    rescue SignalException => error
      log_error('Signal Exception',
                message: error.message, topic: message.topic, payload: print_payload, backtrace: error.backtrace)
      @consumer.stop
    end
  end
end
