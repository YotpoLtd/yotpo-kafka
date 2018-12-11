require File.expand_path('../../lib/yotpo_kafka', __FILE__)
require 'rspec-rails'

describe YotpoKafka::Producer do
  before(:each) do
    a = 1
  end

  it 'should succeed to connect to Kafka' do
    YotpoKafka::Producer.new({enqueue_on_fail: true, kafka_broker_url: GEM_CONF['default']['SERVICE_ADDRESS']})
  end
end