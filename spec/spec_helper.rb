require 'rspec'
require 'yotpo_kafka'
require 'helpers/consumer_helper'

RSpec.configure do |config|
  config.expect_with :rspec do |c|
    c.syntax = :expect
  end
end
