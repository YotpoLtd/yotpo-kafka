require 'ylogger'

module YotpoKafka
  class YLoggerKafka
    def self.config(is_logstash_logger)
      Ylogger.configure do |config|
        log_info('logstash is configured', log_tag: 'yotpo-ruby-kafka')
        config.logstash_logger = is_logstash_logger
      end
    end
  end
end
