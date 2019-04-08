require 'spec_helper'
require 'kafka'

describe YotpoKafka do
  before(:each) do
    @topic = 'test_topic'
    @message = { test_message: 'testing kafka' }
    @messages = %w[a b c]
  end

  it 'config a producer without parameters works' do
    expect { YotpoKafka::Producer.new({}) }.not_to raise_error
  end

  it 'publish message without headers' do
    producer = YotpoKafka::Producer.new({})
    expect { producer.publish(@topic, @message) }.not_to raise_error
  end

  it 'publish message with headers' do
    producer = YotpoKafka::Producer.new({})
    headers = { hdr: 'headers' }
    key = 'key'
    expect { producer.publish(@topic, @message, headers, key) }.not_to raise_error
  end

  it 'publish message with headers' do
    producer = YotpoKafka::Producer.new({})
    headers = { hdr: 'headers' }
    key = 'key'
    expect { producer.publish_multiple(@topic, @messages, headers, key) }.not_to raise_error
  end
end
