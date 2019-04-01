Gem::Specification.new do |s|
  s.name = 'yotpo_kafka'
  s.version = '1.0.0'
  s.date = '2018-11-28'
  s.authors = 'Dana'
  s.summary = 'yotpo_kafka is the best'
  s.files = ['lib/yotpo_kafka.rb']
  s.require_paths = ['lib']

  s.add_dependency 'ruby-kafka', '0.7.6'
  s.add_dependency 'ylogger', '0.1.0'
  s.add_dependency 'rake'
  s.add_development_dependency 'rspec'
end
