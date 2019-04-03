require File.expand_path('../config/application', __FILE__)

require 'rake'

Dir[File.join(File.dirname(__FILE__), 'tasks/*.rake')].each { |rake| load rake }

if %w(test development).include?(ENV['RACK_ENV'])
  begin
    require 'rspec/core'
    require 'rspec/core/rake_task'
    spec = RSpec::Core::RakeTask.new(:spec)
    spec.pattern = 'spec/**{,/*/**}/*_spec.rb'
    require 'rubocop/rake_task'
    RuboCop::RakeTask.new(:rubocop)
  rescue Gem::LoadError => e
    LOGGER.error(e) if defined?(LOGGER)
  end
end

require 'resque/tasks'
require 'mongoid_migrations/tasks'

task default: [:rubocop, :spec]
