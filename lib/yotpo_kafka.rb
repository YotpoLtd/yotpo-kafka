require_relative '../config/initializers/ylogger'
require 'kafka'
require 'yotpo_kafka/producer'
require 'yotpo_kafka/consumer'

module YotpoKafka
  class << self; attr_accessor :kafka; end
  class << self; attr_accessor :retry_topic; end
  class << self; attr_accessor :fatal_topic; end
  class << self; attr_accessor :include_headers; end
  class << self; attr_accessor :retry_header_key; end
end

seed_brokers = ENV['BROKER_URL'] || '127.0.0.1:9092'
YotpoKafka.kafka = Kafka.new(seed_brokers)
YotpoKafka.retry_header_key = 'retry'
YotpoKafka.retry_topic = 'retry_handler'
YotpoKafka.fatal_topic = 'fatal'
YotpoKafka.include_headers = Kafka::FetchedMessage.instance_methods.include?(:headers)
