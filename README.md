# Kafka Gem

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'yotpo_kafka', github: 'YotpoLtd/yotpo-kafka'
```

And then execute:
    $ bundle install
## Creating a producer:

```ruby
new_producer = YotpoKafka::Producer.new({ kafka_broker_url: BROKER})
new_producer.publish(TOPIC_NAME, MESSAGE, KEY = nil, MSG_ID = nil)
```
BROKER = Our Kafka Server
TOPIC_NAME = Name of the topic to publish to
MESSAGE = Message to publish, should be hash
KEY = If messages should be published synchronic, a key should be given. Default: nil
MSG_ID = Message header will include a msg_id. Default: generated unique ID

Additional params possible to send when creating new instance of Producer:
* _**gap_between_retries:**_: In seconds. Default is 0
* _**num_retries:**_: Num of retries before failure is sent to fatal topic. Default is 0
* _**client_id:**_: Unique identifier of the publisher. Default is 'yotpo-kafka'
* _**active_job:**_: Type of job manager (like :resque). Default is nil
* _**red_cross:**_: Monitoring by red cross. Default is nil
* _**logstash_logger:**_:  If set to true, will log in Logstash format. Default is true

Retry Mechanism of Producer: 
In case producer fails to produce for any reason (and defined an Active Job) a job will be sent
to relevant Jobs manager. It will try to republish the message after waiting gap_between_retries seconds
as defined. Once number of retries is 0, it will try to publish the message to topic named TOPIC_NAME_fatal. 

## Creating a consumer:
A consumer will be defined in a rake task as follows:

```ruby
desc 'New Consumer'
  task :new_consumer do
    begin
      params = {kafka_broker_url: BROKER,
                topic: TOPIC_NAME,
                group_id: GROUP_ID,
                handler: CONSUMER_CLASS
      }
      NewConsumer.start_consumer(params)
    end
  end
```
BROKER = Our Kafka Server
TOPIC_NAME = Name of the topic to publish to
GROUP_ID = Consumer will be part of consumer group with given id
CONSUMER_CLASS = Class that handles the received messages

Retry Mechanism of Consumer:
In case there is an exception in consuming message (and defined an Active Job), there will be
reproduction of the message to a topic called: TOPIC_NAME_GROUP_ID_failures. 
When retry is 0, message will be sent to a topic called: TOPIC_NAME_GROUP_ID_failures.

#Defining the handler (Consumer class):
CONSUMER_CLASS should inherit from YotpoKafka::Consumer 
and define the following (with option to define params):

```ruby
class UpdatedTokenConsumer < YotpoKafka::Consumer
  REQUIRED_PARAMS = %i(app_key token invalidated_at).freeze

  def initialize(params = {})
    params = {'gap_between_retries' =>  2,
              'num_retries' =>  2,
              'logstash_logger' => false,
              'active_job' => :resque}
    super(params)
  end

  def consume_message(message)
    #do_something_with_message
  end
end
```
Additional params possible to send when creating new instance of Consumer:
* _**gap_between_retries:**_: In seconds. Default is 0
* _**num_retries:**_: Num of retries of reconsuming message in case of exception. 
                       When retry is 0, failure is sent to fatal topic. Default is 0
* _**client_id:**_: Unique identifier of the publisher. Default is 'yotpo-kafka'
* _**active_job:**_: Type of job manager (like :resque). Default is nil
* _**red_cross:**_: Monitoring by red cross. Default is nil
* _**logstash_logger:**_:  If set to true, will log in Logstash format. Default is true


  ### Dependencies
  
  * `Ruby >= 2.2`
  * source 'https://yotpo.jfrog.io/yotpo/api/gems/gem-virt/' in Gemfile
  
  #### How to install Kafka locally for debugging needs:
  1. brew cask install java8
  2. brew install kafka
  
  3. vim /usr/local/etc/kafka/server.properties
  Here uncomment the server settings and update the value from:
  
  listeners=PLAINTEXT://:9092
  to
  
                Socket Server Settings 
  The address the socket server listens on. It will get the value returned from 
  java.net.InetAddress.getCanonicalHostName() if not configured.
    FORMAT:
      listeners = listener_name://host_name:port
    EXAMPLE:
      listeners = PLAINTEXT://your.host.name:9092
  listeners=PLAINTEXT://localhost:9092
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