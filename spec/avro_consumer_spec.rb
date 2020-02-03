require 'spec_helper'

describe YotpoKafka::AvroConsumer do

  it 'init AvroConsumer with DEFAULT_SCHEMA_REGISTRY_URL' do
    YotpoKafka.kafka_v2 = true

    expect(AvroTurf::Messaging).to receive(:new).with(registry_url: YotpoKafka::AvroConsumer::DEFAULT_SCHEMA_REGISTRY_URL)

    YotpoKafka::AvroConsumer.new({})
  end

  it 'init AvroConsumer with env var REGISTRY_URL' do
    YotpoKafka.kafka_v2 = true
    dummy_schema_registry_url = 'http://dummy-schema-registry.com'
    ENV['REGISTRY_URL'] = dummy_schema_registry_url

    expect(AvroTurf::Messaging).to receive(:new).with(registry_url: dummy_schema_registry_url)

    YotpoKafka::AvroConsumer.new({})
  end

  it 'extract the message value and call avro_messaging.decode with the value' do
    message_value = 'message_value'
    message = OpenStruct.new(value: message_value)
    avro_messaging = Helpers::AvroMessaging.new
    allow(AvroTurf::Messaging).to receive(:new).and_return(avro_messaging)
    avro_consumer = YotpoKafka::AvroConsumer.new({})

    expect(avro_messaging).to receive(:decode).with(message_value)

    avro_consumer.extract_payload(message)
  end

  it 'consume_message with message payload' do
    message_value = 'message_value'
    avro_messaging = Helpers::AvroMessaging.new
    decoded_message_value = avro_messaging.decode(message_value)
    message = OpenStruct.new(value: message_value, topic: 'topic')
    allow(AvroTurf::Messaging).to receive(:new).and_return(avro_messaging)
    avro_consumer = YotpoKafka::AvroConsumer.new({json_parse: false})

    allow(avro_messaging).to receive(:decode).with(message_value)
    expect_any_instance_of(YotpoKafka::BaseConsumer).to receive(:consume_message).with(decoded_message_value)

    avro_consumer.handle_consume(decoded_message_value, message)
  end
end

