# Kafka Gem

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'yotpo-ruby-kafka'
```

Define BROKER_URL environment variable (default 127.0.0.1)

### Creating a producer:

```ruby
YotpoKafka::Producer.new.publish(EVENTBUS['REVIEW_IMAGE_CREATE_TOPIC'], message, headers, key)
```

* **_value:_** string to publish

* **_topic:_** name of the topic to publish to (can also be an array of topics)

* _**headers:**_ kafka headers map

* _**key:**_ messages with same key will go to same partition. Order within
        a partition is ensured and therefore all messages with same key
        will be sent synchronicly. Advised to use when order of messages
        is required.Default: nil
        
* _**red_cross:**_: monitoring by red cross. Default is false

* _**logstash_logger:**_:  if set to true, will log in Logstash format. indexing uuid, 
                            last few backtrace lines as context,
                            tag, and an extra_data hash provided to the logger by the user.. Default is true

### Creating a consumer:
A consumer will be defined in a rake task as follows:

```ruby
  desc 'New Consumer'
  task :new_consumer do
      Consumers::DummyConsumer.new({
                                    seconds_between_retries: 10,
                                    num_retries: 3,
                                    topics: 'rubytest',
                                    group_id: 'consumer_test_topics',
                                   }).start_consumer
  end
```
* _**topics:**_ name of the topic to publish to (or an array of topics)

* **_group_id:_** consumer will be part of consumer group with given id (One for all topics)

* **_handler:_** class that handles the consumed messages payload
```ruby
require 'yotpo_kafka'

module Consumers
  class ApplicationNameFromCode < YotpoKafka::Consumer
    def consume_message(_message);
    end
  end
end
```
* _**seconds_between_retries:**_ in seconds.

* _**num_retries:**_ num of retries of reconsuming message in case of exception. 
                       When retry is 0, failure is sent to fatal topic. Default is 0
                       
* _**red_cross:**_ monitoring by red cross. Default is nil

* _**logstash_logger:**_ if set to true, will log in Logstash format. Default is true

#### Retry Mechanism
Check [kafka-retry-service](https://github.com/YotpoLtd/kafka-retry-service) for more details 
  
#### How to install Kafka locally for debugging needs:
```
1. brew cask install java8
2. brew install kafka

3. vim /usr/local/etc/kafka/server.properties
Here uncomment the server settings and update the value from:

listeners=PLAINTEXT://:9092
to
listeners = PLAINTEXT://your.host.name:9092
and restart the server and it will work great.

4. zookeeper-server-start /usr/local/etc/kafka/zookeeper.properties
kafka-server-start /usr/local/etc/kafka/server.properties

5. Create Kafka Topic:
kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

6. Initialize Producer console:
kafka-console-producer --broker-list localhost:9092 --topic test

7. Initialize Consumer console:
kafka-console-consumer --bootstrap-server localhost:9092 --topic test --from-beginning
```

#### Using dockers
```
docker-compose -f docker-compose/docker-compose.yml up -d
```

### Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/[USERNAME]/yotpo-kafka. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [Contributor Covenant](http://contributor-covenant.org) code of conduct.

### License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

### Code of Conduct

Everyone interacting in the YotpoTestWorkflow project’s codebases, issue trackers, chat rooms and mailing lists is expected to follow the [code of conduct](https://github.com/[USERNAME]/yotpo_test_workflow/blob/master/CODE_OF_CONDUCT.md).