require 'kafka'
require 'ylogger'

module YotpoKafka
  class Consumer
    extend Ylogger

    def initialize(params)
      @seconds_between_retries = params[:seconds_between_retries] || 0
      @num_retries = params[:num_retries] || 0
      @red_cross = params[:red_cross] || nil
      @logstash_logger = params[:logstash_logger] || false
      @topics = Array(params[:topics]) || nil
      @group_id = params[:group_id]
      @consumer = YotpoKafka.kafka.consumer(group_id: @group_id)
      @producer = Producer.new({
                                 red_cross: @red_cross,
                                 client_id: @group_id,
                                 logstash_logger: @logstash_logger,
                               })
      config
    rescue => error
      log_error('Could not initialize', error)
      raise 'Could not initialize'
    end

    def start_consumer
      YotpoKafka::YLoggerKafka.config(@logstash_logger || true)
      @topics = add_failure_topics
      @topics.each { |t| @consumer.subscribe(t) }
      @consumer.each_message do |message|
        @consumer.mark_message_as_processed(message)
        begin
          consume_message(message.value)
          log_info('Message consumed', topic: message.topic, log_tag: 'yotpo-kafka')
          RedCross.monitor_track(event: 'messageConsumed', properties: { success: true }) unless @red_cross.nil?
        rescue => error
          RedCross.monitor_track(event: 'messageConsumed', properties: { success: false }) unless @red_cross.nil?
          handle_error(message, error)
        end
      end
    rescue => error
      log_error('Consumer failed',
                exception: error,
                log_tag: 'yotpo-kafka')
      # graceful shutdown.. restart service to consume again
    end

    def add_failure_topics
      all_topics = []
      @topics.each { |t| all_topics += [build_fail_topic(t), t] }
      all_topics
    end

    def build_fail_topic(topic)
      return @group_id + '.failures'
    end

    def handle_error(message, error)
      if !message.headers['retry']
        message.headers['retry'] = {
          CurrentAttempt: @num_retries,
          NextExecTime: (Time.now.utc + @seconds_between_retries).to_datetime.rfc3339,
          Error: error.to_s,
          MainTopic: message.topic,
          FailuresTopic: build_fail_topic(message.topic),
          delayIntervalSec: @seconds_between_retries,
        }.to_json
      end
      parsed_hdr = JSON.parse(message.headers['retry'])
      retry_hdr = {
        CurrentAttempt: parsed_hdr['CurrentAttempt'] - 1,
        NextExecTime: (Time.now.utc + @seconds_between_retries).to_datetime.rfc3339,
        Error: error.to_s,
        MainTopic: message.topic,
        FailuresTopic: build_fail_topic(message.topic),
      }
      if retry_hdr[:CurrentAttempt] > 0
        message.headers['retry'] = retry_hdr.to_json
        log_info('Message was not consumed - wait for retry', { topic: message.topic })
        if @seconds_between_retries == 0
          @producer.publish(message.value, message.headers, build_fail_topic(message.topic), message.key)
        else
          @producer.publish(message.value, message.headers,'retry_handler', message.key)
        end
      else
        retry_hdr[:NextExecTime] = Time.now.utc.to_datetime.rfc3339
        message.headers['retry'] = retry_hdr.to_json
        log_info('Message was not consumed - sent to fatal', { topic: message.topic })
        @producer.publish(message.value, message.headers, 'fatal', message.key)
      end
    end

    def config
      YotpoKafka::RedCrossKafka.config(@red_cross)
      YotpoKafka::YLoggerKafka.config(@logstash_logger)
    rescue => error
      log_error('Could not config', exception: error)
      raise 'Could not config'
    end

    def consume_message(_message)
      raise NotImplementedError
    end
  end
end
