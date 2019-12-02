require 'spec_helper'
require 'kafka'

describe YotpoKafka do
  before(:each) do
    @topic = 'test_topic'
    @message = { test_message: 'testing kafka' }
    @messages = %w[a b c]
    @kafka = Kafka.new('127.0.0.1:9092')
  end

  it 'config a producer without parameters works' do
    expect { YotpoKafka::Producer.new({}) }.not_to raise_error
  end

  it 'publish message without headers' do
    producer = YotpoKafka::Producer.new({})
    expect { producer.publish(@topic, @message) }.not_to raise_error
  end

  it 'publish message not utf-8 characters' do
    producer = YotpoKafka::Producer.new({})
    expect { producer.publish(@topic, 128.chr) }.not_to raise_error
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

  it 'publish with retry check' do
    producer = YotpoKafka::Producer.new({})
    headers = { hdr: 'headers' }
    key = 'key'
    @kafka = Kafka.new('127.0.0.1:9999')
    allow(YotpoKafka::Producer).to receive(:publish).with(any_args).exactly(3).times
    allow(RestClient).to receive(:post).with(any_args).exactly(1).times
    expect { producer.async_publish_with_retry(@topic, @messages, headers, key) }.not_to raise_error
    expect_any_instance_of(Kafka::Producer).to receive(:produce)
      .with(@messages.to_json, headers: headers, key: 'key', topic: @topic).times(3).and_raise
    thread = producer.async_publish_with_retry(@topic, @messages, headers, key, 3)
    thread.join
  end

  it 'when publish with retry success it doesnt send rest request' do
    producer = YotpoKafka::Producer.new({})
    headers = { hdr: 'headers' }
    key = 'key'
    expect_any_instance_of(Kafka::Producer).to receive(:produce)
      .with(@messages.to_json, headers: headers, key: 'key', topic: @topic).exactly(1).times
    expect(RestClient).to receive(:post).with(any_args).exactly(0).times
    thread = producer.async_publish_with_retry(@topic, @messages, headers, key)
    thread.join
  end
end
