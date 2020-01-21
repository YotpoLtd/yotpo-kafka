require 'rspec'
require 'yotpo_kafka'
require 'helpers/consumer_helper'
require 'helpers/avro_messaging'
require 'ostruct'
RSpec.configure do |config|
  config.expect_with :rspec do |c|
    c.syntax = :expect
  end
end
