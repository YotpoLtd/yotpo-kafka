require 'active_job'

module YotpoKafka
  class ActiveJobs
    def self.config(adapter)
      ActiveJob::Base.queue_adapter = adapter unless adapter.nil?
    end
  end
end