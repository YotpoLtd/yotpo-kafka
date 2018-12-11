require 'phobos/cli/runner'

module YotpoKafka
  class ConsumerRunner
    class << self
      def run(params)
        YotpoKafka::ConsumerConfig.configure(params)
        runner = Phobos::CLI::Runner.new
        runner.run!
      rescue => error
        raise "An error occurred , error: #{error}"
      end
    end
  end
end