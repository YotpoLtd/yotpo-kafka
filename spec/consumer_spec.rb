require 'spec_helper'

describe YotpoKafka do
  before(:each) do
    allow_any_instance_of(Kafka).to receive(:create_topic)
    YotpoKafka.seed_brokers = '127.0.0.1:9092'
    YotpoKafka.default_replication_factor = 1
    YotpoKafka.default_partitions_num = 1
  end

  it 'config empty consumer without handler expects runtime error' do
    expect { YotpoKafka::Consumer.new({}) }.not_to raise_error
  end

  it 'config a consumer without topic' do
    consumer = YotpoKafka::Consumer.new({})
    expect { consumer.start_consumer }.to_not raise_error
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
end
