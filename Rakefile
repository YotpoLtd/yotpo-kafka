require 'bundler/gem_tasks'
require 'rubocop/rake_task'

if %w[test development].include?(ENV['RACK_ENV'])
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

RuboCop::RakeTask.new(:rubocop)
task default: %i[rubocop spec]
