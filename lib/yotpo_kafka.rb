require_relative '../config/initializers/ylogger'
require 'kafka'
require 'yotpo_kafka/producer'
require 'yotpo_kafka/consumer'

module YotpoKafka
  class << self; attr_accessor :kafka_retry_service_url; end
  class << self; attr_accessor :kafka; end
  class << self; attr_accessor :retry_topic; end
  class << self; attr_accessor :fatal_topic; end
  class << self; attr_accessor :failures_topic_suffix; end
  class << self; attr_accessor :kafka_v2; end
  class << self; attr_accessor :retry_header_key; end
  class << self; attr_accessor :default_partitions_num; end
  class << self; attr_accessor :default_replication_factor; end
end

YotpoKafka.kafka_retry_service_url = ENV['KAFKA_RETRY_SERVICE_URL'] || '127.0.0.1:8080'
YotpoKafka.retry_header_key = 'retry'
YotpoKafka.retry_topic = 'retry_handler'
YotpoKafka.fatal_topic = 'fatal'
YotpoKafka.failures_topic_suffix = '.failures'
YotpoKafka.kafka_v2 = Kafka::FetchedMessage.instance_methods.include?(:headers)
YotpoKafka.default_partitions_num = ENV['DEFAULT_PARTITIONS_NUM'] || 1
YotpoKafka.default_replication_factor = ENV['DEFAULT_REPLICATION_FACTOR'] || 1
