RedCross::Configuration.configure do |config|
  config.trackers = {
      monitor: RedCross::Trackers::MonitorTracker.new(ENV['YOTPO']['INFLUXDB_DB'],
                                                      ENV['YOTPO']['INFLUXDB_HOST'],
                                                      ENV['YOTPO']['INFLUXDB_PORT'])

  }
  config.default_tracker = :monitor
end
