ENV['BROKER_URL'] = '127.0.0.1:9092'

require 'spec_helper'

describe YotpoKafka do
  before(:each) do
    @topic = 'test_topic'
    @client_id = 'kafka-test'
    @num_retries = 1
    @message = { test_message: 'testing kafka' }
    @error = 'error'
  end

  it 'config a producer without broker expects runtime error' do
    expect { YotpoKafka::Producer.new }.to raise_error(RuntimeError)
  end

  it 'config a producer properly' do
    expect { YotpoKafka::Producer.new(kafka_broker_url: @kafka_broker_url) }.not_to raise_error
  end

  it 'publish message without kafka header to return message with a kafka header' do
    allow_any_instance_of(Phobos::Producer).to receive(:publish_list)
    allow_any_instance_of(Phobos::Listener).to receive(:close)
    allow_any_instance_of(YotpoKafka::Producer).to receive(:publish_messages)

    producer = YotpoKafka::Producer.new(kafka_broker_url: @kafka_broker_url)
    producer.publish(@topic, @message)
    expect(@message['kafka_header'][:kafka_broker_url]).to eq(@kafka_broker_url)
  end

  it 'publish message with kafka header to return message with given timestamp in kafka header' do
    allow_any_instance_of(Phobos::Producer).to receive(:publish_list)
    allow_any_instance_of(Phobos::Listener).to receive(:close)

    producer = YotpoKafka::Producer.new(kafka_broker_url: @kafka_broker_url)
    curr_time = DateTime.now
    @message['kafka_header'] = { timestamp: curr_time }
    producer.publish(@topic, @message)
    expect(@message['kafka_header'][:timestamp]).to eq(curr_time)
  end

  it 'publish multiple messages that will receive Kafka_header' do
    allow_any_instance_of(Phobos::Producer).to receive(:publish_list)
    allow_any_instance_of(Phobos::Listener).to receive(:close)

    producer = YotpoKafka::Producer.new(kafka_broker_url: @kafka_broker_url)
    messages = [{ topic: @topic, message: @message }, { topic: @topic, message: @message }]
    producer.publish_multiple(messages)
    messages.each do |_message|
      expect(@message['kafka_header'][:kafka_broker_url]).to eq(@kafka_broker_url)
    end
  end

  it 'raises error when trying to enqueue without an active job' do
    producer = YotpoKafka::Producer.new(kafka_broker_url: @kafka_broker_url)
    expect { producer.enqueue(@message, @topic, nil, nil, @error) }.to raise_error(NotImplementedError)
  end

  it 'works properly when enqueue failed to produce and has resque as active job' do
    producer = YotpoKafka::Producer.new(kafka_broker_url: @kafka_broker_url, active_job: @active_job)
    expect { producer.enqueue(@message, @topic, nil, nil, @error) }.not_to raise_error
  end
end
