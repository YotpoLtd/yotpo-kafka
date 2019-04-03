ENV['BROKER_URL'] = '192.0.0.1:9092'

require 'spec_helper'

describe YotpoKafka do
  before(:each) do
    @topic = 'test_topic'
    @client_id = 'kafka-test'
    @group_id = 1
    @message = { test_message: 'testing kafka' }
    @error = 'error'
  end

  it 'config a consumer without handler expects runtime error' do
    params = { topic: @topic,
               group_id: @group_id,
               handler: nil
    }
    expect { Helpers::ConsumerHandler.new(params) }.to raise_error(ArgumentError)
  end

  it 'config a consumer with handler expects no runtime error' do
    allow_any_instance_of(Phobos::CLI::Runner).to receive(:run!)
    params = { topic: @topic,
               group_id: @group_id,
               handler: Helpers::ConsumerHandler
    }
    consumer = Helpers::ConsumerHandler.new(params)
    expect { consumer.start_consumer }.to_not raise_error
  end

  it 'does not raise error when message is not a json' do
    metadata = { topic: @topic, handler: Helpers::ConsumerHandler }
    consumer = Helpers::ConsumerHandler.new({})
    expect { consumer.consume(@message.to_json, metadata) }.to_not raise_error
  end

  it 'raises error when trying to enqueue without an active job' do
    allow_any_instance_of(YotpoKafka::Consumer).to receive(:get_broker)
    consumer = Helpers::ConsumerHandler.new({})
    expect { consumer.enqueue(@message, @topic, @error) }.to raise_error(NotImplementedError)
  end

  it 'raises error when trying to enqueue without an active job' do
    allow_any_instance_of(YotpoKafka::Consumer).to receive(:get_broker)
    consumer = Helpers::ConsumerHandler.new(active_job: :resque)
    expect { consumer.enqueue(@message, @topic, @error) }.to_not raise_error
  end

  it 'expects num of retries to be as defined in consumer_helper when kafka_header does not exist' do
    consumer = Helpers::ConsumerHandler.new({})
    consumer.calc_num_of_retries(@message)
    expect(@message['kafka_header']['num_retries']).to eq(consumer.num_retries)
  end

  it 'expects num of retries to be as defined in consumer_helper when kafka_header has num_retries nil' do
    consumer = Helpers::ConsumerHandler.new({})
    @message['kafka_header'] = { 'num_retries' => nil }
    consumer.calc_num_of_retries(@message)
    expect(@message['kafka_header']['num_retries']).to eq(consumer.num_retries)
  end

  it 'expects num of retries to reduce by one' do
    consumer = Helpers::ConsumerHandler.new({})
    @message['kafka_header'] = { 'num_retries' => consumer.num_retries }
    consumer.calc_num_of_retries(@message)
    expect(@message['kafka_header']['num_retries']).to eq(consumer.num_retries - 1)
  end

  it 'expects the topic to be enqueued to contain fatal' do
    consumer = Helpers::ConsumerHandler.new({})
    @message['kafka_header'] = { 'num_retries' => 0 }
    topic_to_enqueue = consumer.get_topic_to_enqueue(@message, topic: @topic, group_id: @group_id)
    expect(topic_to_enqueue).to include('fatal')
  end

  it 'expects the topic to be enqueued to contain _failures' do
    consumer = Helpers::ConsumerHandler.new({})
    @message['kafka_header'] = { 'num_retries' => consumer.num_retries }
    topic_to_enqueue = consumer.get_topic_to_enqueue(@message, topic: @topic, group_id: @group_id)
    expect(topic_to_enqueue).to include('_failures')
  end

  it 'expects the topic to be enqueued to be the given topic' do
    consumer = Helpers::ConsumerHandler.new({})
    @message['kafka_header'] = { 'num_retries' => consumer.num_retries - 1 }
    topic_to_enqueue = consumer.get_topic_to_enqueue(@message, topic: @topic, group_id: @group_id)
    expect(topic_to_enqueue).to eq(@topic)
  end

  it 'expects the topic to be enqueued to be the nil' do
    consumer = Helpers::ConsumerHandler.new({})
    @message['kafka_header'] = { 'num_retries' => -1 }
    topic_to_enqueue = consumer.get_topic_to_enqueue(@message, topic: @topic, group_id: @group_id)
    expect(topic_to_enqueue).to eq(nil)
  end
end
