require_relative '../config/initializers/red_cross_monitor'
require_relative '../config/initializers/ylogger'
require 'kafka'
require 'yotpo_kafka/producer'
require 'yotpo_kafka/consumer'

module YotpoKafka
  class << self; attr_accessor :kafka; end
  class << self; attr_accessor :retry_topic; end
  class << self; attr_accessor :fatal_topic; end
end

YotpoKafka.kafka = Kafka.new(ENV['BROKER_URL'])
YotpoKafka.retry_topic = 'retry_handler'
YotpoKafka.fatal_topic = 'fatal'
