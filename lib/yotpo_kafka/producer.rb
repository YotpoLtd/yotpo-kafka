require 'kafka'
require 'date'
require 'securerandom'
require 'ylogger'
require 'json'
require 'rest-client'
require 'avro_turf/messaging'

module YotpoKafka
  class Producer
    extend Ylogger

    def initialize(params = {})
      YotpoKafka::YLoggerKafka.config(true)
      set_log_tag(:yotpo_ruby_kafka)
      @producer = YotpoKafka.kafka.producer
      @avro_encoding = params[:avro_encoding] || false
      @avro = nil
      @red_cross = params[:red_cross] || false
    rescue => error
      log_error('Producer failed to initialize',
                exception: error.message,
                broker_url: YotpoKafka.seed_brokers)
      raise 'Producer failed to initialize'
    end

    def set_avro_registry(registry_url)
      @avro = AvroTurf::Messaging.new(registry_url: registry_url)
    end

    def publish(topic, payload, kafka_v2_headers = {}, key = nil, to_json = true)
      log_info('Publishing message',
               topic: topic, message: payload, headers: kafka_v2_headers, key: key, broker_url: YotpoKafka.seed_brokers)
      payload = payload.to_json if to_json
      if @avro_encoding
        raise 'avro schema is not set' unless @avro

        schema = topic
        if schema.include? YotpoKafka.failures_topic_suffix
          if YotpoKafka.kafka_v2
            message.headers[YotpoKafka.retry_header_key]['MainTopic']
          else
            JSON.parse(message.value)['MainTopic']
          end
        end
        payload = @avro.decode(payload, schema_name: schema)
      end
      if YotpoKafka.kafka_v2
        @producer.produce(payload, key: key, headers: kafka_v2_headers, topic: topic)
      else
        @producer.produce(payload, key: key, topic: topic)
      end
      @producer.deliver_messages
    rescue => error
      log_error('Single publish failed',
                broker_url: YotpoKafka.seed_brokers,
                message: payload,
                headers: kafka_v2_headers,
                topic: topic,
                error: error.message)
      raise error
    end

    def async_publish_with_retry(topic, value, headers = {}, key = nil,
                                 immediate_retry_count = 3, interval_between_retry = 2)
      backtrace_keeper = caller
      backtrace_keeper = backtrace_keeper[0..5] if backtrace_keeper.length > 6
      is_published = false
      last_error = ''
      thread = Thread.new {
        (1..immediate_retry_count).each do |try_num|
          begin
            publish(topic, value, headers, key)
            is_published = true
            break
          rescue => error
            log_error('Async publish failed, attempt: ' + try_num.to_s,
                      topic: topic, message: value, headers: headers, key: key, broker_url: YotpoKafka.seed_brokers,
                      error: error.message,
                      backtrace: backtrace_keeper)
            sleep(interval_between_retry)
            last_error = error.message
          end
        end
        begin
          unless is_published
            RestClient.post(YotpoKafka.kafka_retry_service_url + '/v1/kafkaretry/produce_errors', {
              'produce_time': Time.now.utc.to_datetime.rfc3339,
              'error_msg': last_error,
              'topic': topic,
              'payload': value
            }.to_json, headers = { content_type: 'application/json' })
            log_info('Saved failed publish',
                     error: error.message,
                     kafka_retry_service_url: YotpoKafka.kafka_retry_service_url,
                     last_produce_error: last_error,
                     topic: topic,
                     payload: value)
          end
        rescue => error
          log_error('Save publish error failed',
                    error: error.message,
                    kafka_retry_service_url: YotpoKafka.kafka_retry_service_url,
                    last_produce_error: last_error,
                    topic: topic,
                    payload: value)
        end
      }
      thread
    end

    def publish_multiple(topic, payloads, kafka_v2_headers = {}, key = nil, to_json = true)
      log_info('Publishing multiple messages',
               topic: topic, message: value, headers: kafka_v2_headers, key: key, broker_url: YotpoKafka.seed_brokers)
      payloads.each do |payload|
        publish(topic, payload, kafka_v2_headers, key, to_json)
      end
      RedCross.monitor_track(event: 'messagePublished', properties: { success: true }) if @red_cross
    rescue => error
      log_error('Publish multi messages failed',
                exception: error.message)
      RedCross.monitor_track(event: 'messagePublished', properties: { success: false }) if @red_cross
    end
  end
end
