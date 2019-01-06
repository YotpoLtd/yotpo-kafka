module YotpoKafka
  class RedCrossKafka
    def self.config(params)
      unless params.nil?
        RedCross::Configuration.configure do |config|
          config.trackers = {
              monitor: RedCross::Trackers::MonitorTracker.new(params[:db],
                                                              params[:host],
                                                              params[:port])

          }
          config.default_tracker = :monitor
        end
      end
    end
  end
end