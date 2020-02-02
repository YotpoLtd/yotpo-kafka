require 'spec_helper'

describe YotpoKafka do
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

  it 'config consumer with listen_to_failures=true and json_parse=false resulting listen_to_failures=false for kafka_v1' do
    YotpoKafka.kafka_v2 = false

    consumer = YotpoKafka::Consumer.new(listen_to_failures: true, json_parse: false)

    expect(consumer.instance_variable_get(:@listen_to_failures)).to be(false)
  end

  it 'consumer one topic with failures topic' do
    consumer = YotpoKafka::Consumer.new(topics: 'blue')
    expect { consumer.subscribe_to_topics }.to_not raise_error
  end

  it 'consumer one topic without failures topic' do
    consumer = YotpoKafka::Consumer.new(topics: 'blue2', listen_to_failures: false)
    expect { consumer.subscribe_to_topics }.to_not raise_error
  end

  it 'consumer to multi topics' do
    consumer = YotpoKafka::Consumer.new(topics: %w[blue yaniv magniv])
    expect { consumer.subscribe_to_topics }.to_not raise_error
  end

  it 'consumer to multi topics' do
    consumer = YotpoKafka::Consumer.new(topics: %w[blue yaniv magniv], group_id: 'my_group')
    expect { consumer.subscribe_to_topics }.to_not raise_error
  end

  it 'getting to consume message' do
    expect { Helpers::ConsumerHandler.new.consume_message('message') }.to_not raise_error
  end

  it 'getting to consume message' do
    expect { Helpers::ConsumerHandler.new.handle_consume('message', DummyMsg.new) }.to_not raise_error
  end

  it 'getting to consume message that raises error, key nil' do
    YotpoKafka.kafka_v2 = true
    expect { Helpers::ConsumerHandlerWithError.new.handle_consume('message', DummyMsg.new(key: nil)) }.to_not raise_error
  end

  it 'getting to consume message that raises error, key not nil' do
    YotpoKafka.kafka_v2 = true
    expect { Helpers::ConsumerHandlerWithError.new.handle_consume('message', DummyMsg.new(key: 'buya')) }.to_not raise_error
  end

  it 'getting to consume message that raises error, key not utf chars' do
    expect { Helpers::ConsumerHandlerWithError.new.handle_consume('message', DummyMsg.new(key: 128.chr)) }.to_not raise_error
  end

  it 'consumer create failure topic according to setting' do
    consumer = YotpoKafka::Consumer.new(topics: 'blue2', listen_to_failures: true)
    expect(consumer.kafka).to receive(:create_topic).once
    consumer.subscribe_to_topics
  end

  it 'consumer doesnt create failure topic according to setting' do
    consumer = YotpoKafka::Consumer.new(topics: 'blue2', listen_to_failures: false)
    expect(consumer.kafka).to receive(:create_topic).never
    consumer.subscribe_to_topics
  end

  it 'handle error if consumer raises' do
    YotpoKafka.kafka_v2 = true
    expect_any_instance_of(YotpoKafka::Consumer).to receive(:handle_error_kafka_v2).once.and_call_original
    expect_any_instance_of(YotpoKafka::Consumer).to receive(:publish_to_retry_service).once.and_call_original
    expect_any_instance_of(YotpoKafka::Producer).to receive(:publish)
    Helpers::ConsumerHandlerWithError.new.handle_consume('message', DummyMsg.new(key: 'buya'))
  end

  it 'sets failure headers if error' do
    YotpoKafka.kafka_v2 = true
    message = DummyMsg.new(key: 'buya')
    Helpers::ConsumerHandlerWithError.new.handle_consume('message', message)
    headers = JSON.parse(message.headers['retry'])
    expect(headers['CurrentAttempt']).to eq(-1)
  end

  it 'reduce attempts in headers if error' do
    YotpoKafka.kafka_v2 = true
    expect_any_instance_of(YotpoKafka::Producer).to receive(:publish).with('topic.missing_groupid.failures', anything, anything, anything, anything)
    message = DummyMsg.new(key: 'buya')
    Helpers::ConsumerHandlerWithError.new(num_retries: 3).handle_consume('message', message)
    headers = JSON.parse(message.headers['retry'])
    expect(headers['CurrentAttempt']).to eq(2)
  end

  it 'publish to retry service if seconds between retries' do
    YotpoKafka.kafka_v2 = true
    expect_any_instance_of(YotpoKafka::Producer).to receive(:publish).with('retry_handler', anything, anything, anything, anything)
    message = DummyMsg.new(key: 'buya')
    Helpers::ConsumerHandlerWithError.new(num_retries: 3, seconds_between_retries: 2).handle_consume('message', message)
    headers = JSON.parse(message.headers['retry'])
    expect(headers['CurrentAttempt']).to eq(2)
  end
end
