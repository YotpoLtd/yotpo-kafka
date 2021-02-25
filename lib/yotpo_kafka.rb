require_relative '../config/initializers/ylogger'
require 'kafka'
require 'ylogger'
require 'yotpo_kafka/base_producer'
require 'yotpo_kafka/producer'
require 'yotpo_kafka/avro_producer'
require 'yotpo_kafka/base_consumer'
require 'yotpo_kafka/consumer'
require 'yotpo_kafka/avro_consumer'

module YotpoKafka
  class << self; attr_accessor :kafka_retry_service_url; end
  class << self; attr_accessor :kafka; end
  class << self; attr_accessor :retry_topic; end
  class << self; attr_accessor :fatal_topic; end
  class << self; attr_accessor :failures_topic_suffix; end
  class << self; attr_accessor :retry_header_key; end
end

YotpoKafka.kafka_retry_service_url = ENV['KAFKA_RETRY_SERVICE_URL'] || '127.0.0.1:8080'
YotpoKafka.retry_header_key = 'retry'
YotpoKafka.retry_topic = 'retry_handler'
YotpoKafka.fatal_topic = 'fatal'
YotpoKafka.failures_topic_suffix = '.failures'
