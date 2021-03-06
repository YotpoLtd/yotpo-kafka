require 'ylogger'

module YotpoKafka
  class YLoggerKafka
    extend Ylogger
    def self.config(is_logstash_logger)
      Ylogger.configure do |config|
        config.logstash_logger = is_logstash_logger
      end
    end
  end
end
