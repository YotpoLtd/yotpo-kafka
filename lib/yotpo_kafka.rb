require_relative '../config/initializers/red_cross_monitor'
require_relative '../config/initializers/ylogger'
require 'kafka'
require 'yotpo_kafka/producer'
require 'yotpo_kafka/consumer'

module YotpoKafka
  class << self; attr_accessor :kafka; end
end

YotpoKafka.kafka = Kafka.new(ENV['BROKER_URL'])
