require 'rspec'
require 'helpers/consumer_handler'

require 'yotpo_kafka'

RSpec.configure do |config|
  config.expect_with :rspec do |c|
    c.syntax = :expect
  end
end

module ConsumerHelper

end