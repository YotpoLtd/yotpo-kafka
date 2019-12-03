require 'kafka'
require 'date'
require 'securerandom'
require 'ylogger'
require 'json'
require 'rest-client'

module YotpoKafka
  class Producer
    extend Ylogger

    def initialize(params = {})
      use_logstash_logger = params[:logstash_logger] == false ? false : true
      YotpoKafka::YLoggerKafka.config(use_logstash_logger)
      set_log_tag(:yotpo_ruby_kafka)
      @producer = YotpoKafka.kafka.producer
      @avro_encoding = params[:avro_encoding] || false
    rescue => error
      log_error('Producer failed to initialize',
                exception: error.message,
                broker_url: YotpoKafka.seed_brokers)
      raise 'Producer failed to initialize'
    end

    def set_avro_registry(registry_url)
      require 'avro_turf/messaging'
      @avro = AvroTurf::Messaging.new(registry_url: registry_url)
    end

    def get_printed_payload(payload)
      payload.to_s.encode('UTF-8')
    rescue Encoding::UndefinedConversionError
      'Msg is not encode-able'
    end

    def unsafe_publish(topic, payload, kafka_v2_headers = {}, key = nil, to_json = true)
      payload_print = get_printed_payload(payload)
      log_debug('Publishing message',
                topic: topic, message: payload_print, headers: kafka_v2_headers, key: key,
                broker_url: YotpoKafka.seed_brokers)
      begin
        payload = payload.to_json if to_json
      rescue Encoding::UndefinedConversionError
        log_error('Failed to convert msg to json')
      end
      payload = @avro.encode(payload) if @avro
      handle_produce(payload, key, topic, kafka_v2_headers)
      log_debug('Publish done')
    rescue => error
      log_error('Single publish failed',
                broker_url: YotpoKafka.seed_brokers,
                topic: topic,
                error: error.message)
      raise error
    end

    def publish(topic, payload, kafka_v2_headers = {}, key = nil, to_json = true)
      unsafe_publish(topic, payload, kafka_v2_headers, key, to_json)
    rescue => error
      post_to_retry_service(error, topic, payload, key)
      raise error
    end

    def handle_produce(payload, key, topic, kafka_v2_headers)
      if YotpoKafka.kafka_v2
        @producer.produce(payload, key: key, headers: kafka_v2_headers, topic: topic)
      else
        @producer.produce(payload, key: key, topic: topic)
      end
      @producer.deliver_messages
    end

    def async_publish_with_retry(topic, value, headers = {}, key = nil,
                                 immediate_retry_count = 3, interval_between_retry = 2, to_json = true)
      backtrace_keeper = caller
      backtrace_keeper = backtrace_keeper[0..5] if backtrace_keeper.length > 6
      is_published = false
      last_error = ''
      thread = Thread.new {
        (1..immediate_retry_count).each do |try_num|
          begin
            unsafe_publish(topic, value, headers, key, to_json)
            is_published = true
            break
          rescue => error
            log_error('Async publish failed, attempt: ' + try_num.to_s,
                      topic: topic, broker_url: YotpoKafka.seed_brokers,
                      error: error.message,
                      backtrace: backtrace_keeper)
            sleep(interval_between_retry)
            last_error = error.message
          end
        end
        begin
          post_to_retry_service(last_error, topic, value, key) unless is_published
        rescue => error
          log_error('Save publish error failed',
                    error: error.message,
                    kafka_retry_service_url: YotpoKafka.kafka_retry_service_url,
                    last_produce_error: last_error,
                    topic: topic)
        end
      }
      thread
    end

    def post_to_retry_service(last_error, topic, value, key)
      RestClient.post(YotpoKafka.kafka_retry_service_url + '/v1/kafkaretry/produce_errors', {
        produce_time: Time.now.utc.to_datetime.rfc3339,
        error_msg: last_error,
        topic: topic,
        payload: value,
        key: key
      }.to_json, content_type: 'application/json')
      log_debug('Saved failed publish')
    end

    def publish_multiple(topic, payloads, kafka_v2_headers = {}, key = nil, to_json = true)
      log_debug('Publishing multiple messages',
                topic: topic, message: value, headers: kafka_v2_headers, key: key, broker_url: YotpoKafka.seed_brokers)
      payloads.each do |payload|
        publish(topic, payload, kafka_v2_headers, key, to_json)
      end
    rescue => error
      log_error('Publish multi messages failed',
                exception: error.message)
    end
  end
end
