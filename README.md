# Kafka Gem

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'yotpo_kafka', github: 'YotpoLtd/yotpo-kafka'
```

And then execute:
    $ bundle install

Define BROKER_URL environment variable

## Creating a producer:

```ruby
producer = Producer.new({ red_cross: @red_cross, client_id: @group_id, logstash_logger: @logstash_logger })
producer.publish(value, headers, topic, key)
```

* topic = Name of the topic to publish to (can also be an array of topics)
* value = byte array to publish
* headers = kafka headers
* key = Messages with same key will go to same partition. Order within
        a partition is ensured and therefore all messages with same key
        will be sent synchronicly. Advised to use when order of messages
        is required.Default: nil
* _**red_cross:**_: Monitoring by red cross. Default is nil
* _**logstash_logger:**_:  if set to true, will log in Logstash format. indexing uuid, 
                            last few backtrace lines as context,
                            tag, and an extra_data hash provided to the logger by the user.. Default is true

## Creating a consumer:
A consumer will be defined in a rake task as follows:

```ruby
  desc 'New Consumer'
  task :new_consumer do
    begin
      consumer = Consumers::DummyConsumer.new({
                                                seconds_between_retries: 10,
                                                num_retries: 3,
                                                topics: 'rubytest',
                                                group_id: 'service.consumer_test_topics.consumer',
                                                handler: Consumers::DummyConsumer
                                              })
      consumer.start_consumer
    end
  end
```
* topics = Name of the topic to publish to (or an array of topics)
* group_id = Consumer will be part of consumer group with given id (should be one or exactly as many topics you give)
* handler = Class that handles the consumed messages payload
* _**gap_between_retries:**_: In seconds. Default is 0
* _**num_retries:**_: Num of retries of reconsuming message in case of exception. 
                       When retry is 0, failure is sent to fatal topic. Default is 0
* _**red_cross:**_: Monitoring by red cross. Default is nil
* _**logstash_logger:**_:  If set to true, will log in Logstash format. Default is true

## Retry Mechanism of Consumer:
Check [kafka-retry-service](https://github.com/YotpoLtd/kafka-retry-service) for more details 

#Defining the handler (Consumer class):
CONSUMER_CLASS should inherit from YotpoKafka::Consumer

```ruby
  class DummyConsumer < YotpoKafka::Consumer
    def initialize(params)
      super(params)
    end

    def consume_message(message)
      puts message
      raise "error"
    end
  end
```
  
#### How to install Kafka locally for debugging needs:
1. brew cask install java8
2. brew install kafka

3. vim /usr/local/etc/kafka/server.properties
Here uncomment the server settings and update the value from:

listeners=PLAINTEXT://:9092
to
listeners = PLAINTEXT://your.host.name:9092
and restart the server and it will work great.

4. 
zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties
kafka-server-start /usr/local/etc/kafka/server.properties

5. Create Kafka Topic:
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

6. Initialize Producer console:
kafka-console-producer --broker-list localhost:9092 --topic test

7. Initialize Consumer console:
kafka-console-consumer --bootstrap-server localhost:9092 --topic test --from-beginning

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/[USERNAME]/yotpo-kafka. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [Contributor Covenant](http://contributor-covenant.org) code of conduct.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

## Code of Conduct

Everyone interacting in the YotpoTestWorkflow project’s codebases, issue trackers, chat rooms and mailing lists is expected to follow the [code of conduct](https://github.com/[USERNAME]/yotpo_test_workflow/blob/master/CODE_OF_CONDUCT.md).