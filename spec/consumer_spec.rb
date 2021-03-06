require 'spec_helper'

describe YotpoKafka::Consumer do
  class DummyMsg
    attr_reader :value, :key, :headers, :topic
    def initialize(value: 'value', key: 'key', headers: {}, topic: 'topic')
      @value = value
      @key = key
      @headers = headers
      @topic = topic
      @bytesize = key.to_s.bytesize + value.to_s.bytesize
    end
  end

  before(:each) do
    allow_any_instance_of(Kafka).to receive(:create_topic)
    @seed_brokers = '127.0.0.1:9092'
  end

  it 'config empty consumer without handler expects runtime error' do
    expect { YotpoKafka::Consumer.new({}) }.not_to raise_error
  end

  it 'config a consumer without topic' do
    consumer = YotpoKafka::Consumer.new({})
    expect { consumer.start_consumer }.to_not raise_error
  end

  it 'consumer one topic with failures topic' do
    consumer = YotpoKafka::Consumer.new(topics: 'blue', partitions_num: 1, replication_factor: 1)
    expect { consumer.subscribe_to_topics }.to_not raise_error
  end

  it 'consumer one topic without failures topic' do
    consumer = YotpoKafka::Consumer.new(topics: 'blue2')
    expect { consumer.subscribe_to_topics }.to_not raise_error
  end

  it 'consumer to multi topics' do
    consumer = YotpoKafka::Consumer.new(topics: %w[blue yaniv magniv], partitions_num: 1, replication_factor: 1)
    expect { consumer.subscribe_to_topics }.to_not raise_error
  end

  it 'consumer to multi topics' do
    consumer = YotpoKafka::Consumer.new(topics: %w[blue yaniv magniv], group_id: 'my_group', partitions_num: 1, replication_factor: 1)
    expect { consumer.subscribe_to_topics }.to_not raise_error
  end

  it 'getting to consume message' do
    expect { Helpers::ConsumerHandler.new.consume_message('message') }.to_not raise_error
  end

  it 'getting to consume message' do
    expect { Helpers::ConsumerHandler.new.handle_consume('message', DummyMsg.new) }.to_not raise_error
  end

  it 'getting to consume message that raises error, key nil' do
    expect { Helpers::ConsumerHandlerWithError.new.handle_consume('message', DummyMsg.new(key: nil)) }.to_not raise_error
  end

  it 'getting to consume message that raises error, key not nil' do
    expect { Helpers::ConsumerHandlerWithError.new.handle_consume('message', DummyMsg.new(key: 'buya')) }.to_not raise_error
  end

  it 'getting to consume message that raises error, key not utf chars' do
    expect { Helpers::ConsumerHandlerWithError.new.handle_consume('message', DummyMsg.new(key: 128.chr)) }.to_not raise_error
  end

  it 'handle error if consumer raises' do
    expect_any_instance_of(YotpoKafka::Consumer).to receive(:handle_consume_error).once.and_call_original
    expect_any_instance_of(YotpoKafka::Consumer).to receive(:publish_to_retry_service).once.and_call_original
    expect_any_instance_of(YotpoKafka::Producer).to receive(:publish)
    Helpers::ConsumerHandlerWithError.new(failures_topic: 'test.failures').handle_consume('message', DummyMsg.new(key: 'buya'))
  end

  it 'sets failure headers if error' do
    message = DummyMsg.new(key: 'buya')
    Helpers::ConsumerHandlerWithError.new(failures_topic: 'test.failures').handle_consume('message', message)
    headers = JSON.parse(message.headers['retry'])
    expect(headers['CurrentAttempt']).to eq(-1)
  end

  it 'reduce attempts in headers if error' do
    expect_any_instance_of(YotpoKafka::Producer).to receive(:publish)
      .with(YotpoKafka.retry_topic, anything, anything, anything, anything)
    message = DummyMsg.new(key: 'buya')
    Helpers::ConsumerHandlerWithError.new(num_retries: 3, failures_topic: 'test.failures').handle_consume('message', message)
    headers = JSON.parse(message.headers['retry'])
    expect(headers['CurrentAttempt']).to eq(2)
  end

  it 'publish to retry service if seconds between retries' do
    expect_any_instance_of(YotpoKafka::Producer).to receive(:publish).with('retry_handler', anything, anything, anything, anything)
    message = DummyMsg.new(key: 'buya')
    Helpers::ConsumerHandlerWithError.new(num_retries: 3, seconds_between_retries: 2, failures_topic: 'test.failures').handle_consume('message', message)
    headers = JSON.parse(message.headers['retry'])
    expect(headers['CurrentAttempt']).to eq(2)
  end
end
