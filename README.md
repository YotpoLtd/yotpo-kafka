# YLogger

Kafka Gem

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
new_producer.publish(TOPIC_NAME, MESSAGE)
```
BROKER = Our Kafka Server
TOPIC_NAME = Name of the topic to publish to
MESSAGE = Message to publish, should be hash

Additional params possible to send when creating new instance of Producer:
* _**gap_between_retries:**_: in seconds. Default is 0
* _**num_retries:**_: num of retries before failure is sent to fatal topic. Default is 0
* _**client_id:**_: unique identifier of the publisher. Default is 'yotpo-kafka'
* _**active_job:**_: Type of job manager (like :resque). Default is nil
* _**red_cross:**_: Monitoring by red cross. Default is nil
* _**logstash_logger:**_:  If set to true, will log in Logstash format. Default is true

Retry Mechanism: 
In case producer fails to produce for any reason, if defined an Active Job, a job will be sent
to relevant Jobs manager. It will try to republish the message after waiting gap_between_retries seconds
as defined. Once number of retries is 0, it will try to publish the message to topic named TOPIC_NAME_fatal. 


## Creating a consumer:
A consumer we want to run in the background will be defined in a rake task.


  ### Dependencies
  
  * `Ruby >= 2.2`
  
  #### How to configure:
  Create a `ylogger.rb` file in the `config/initializers` folder and configure gem as such:
  ```ruby
  require 'ylogger'
  
  Ylogger.configure do |config|
    config.logstash_logger = ENV['LOGSTASH_LOGGER']
  end
  ```

## Usage :
    
   For every Class that wants to use the Logger, YLogger has to be extended by that particular Class. For instance:
   
   ```ruby
class FooController < ApplicationController
      extend Ylogger
end 
```

By doing this, you can set the log_tag once per class, while also making all the Ylogger methods accessible from FooController.

You can also provide your logs extra data that can indexed to elastic as such:

```ruby
    log_info("Foo Message", { custom_tag: 'extra_data' })
```
## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/[USERNAME]/ylogger. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [Contributor Covenant](http://contributor-covenant.org) code of conduct.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

## Code of Conduct

Everyone interacting in the YotpoTestWorkflow project’s codebases, issue trackers, chat rooms and mailing lists is expected to follow the [code of conduct](https://github.com/[USERNAME]/yotpo_test_workflow/blob/master/CODE_OF_CONDUCT.md).