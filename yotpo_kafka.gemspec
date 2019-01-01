Gem::Specification.new do |s|
  s.name = %q{yotpo_kafka}
  s.version = "0.0.0"
  s.date = %q{2018-11-28}
  s.authors = "Dana"
  s.summary = %q{yotpo_kafka is the best}
  s.files = ['lib/yotpo_kafka.rb']
  s.require_paths = ["lib"]

  s.add_dependency 'phobos', '1.7.0'
  s.add_dependency 'ruby-kafka', '0.5.3'
  s.add_dependency 'resque', '>= 1.26'
  s.add_dependency 'red_cross'

  s.add_runtime_dependency 'ylogger'
  s.add_dependency 'activejob'
  # s.add_dependency 'rspec-rails', '>= 3.5.0.beta2'
end