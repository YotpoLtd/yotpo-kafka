require 'red_cross'

module YotpoKafka
  class YRedCrossKafka
    def self.config
      RedCross::Configuration.configure do |config|
        config.trackers = {
          monitor: RedCross::Trackers::MonitorTracker.new(ENV['INFLUXDB_DB'],
                                                          ENV['INFLUXDB_HOST'],
                                                          ENV['INFLUXDB_PORT'])

        }
        config.default_tracker = :monitor
      end
    end
  end
end
