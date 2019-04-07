Gem::Specification.new do |s|
  s.name = 'yotpo_kafka'
  s.version = '1.0.2'
  s.date = '2019-04-04'
  s.authors = 'Dana'
  s.summary = 'yotpo-kafka: encapsulate ruby-kafka library with consume retry mechanism'
  s.files = ['lib/yotpo_kafka.rb']
  s.require_paths = ['lib']

  s.add_dependency 'rake'
  s.add_dependency 'ruby-kafka', '0.7.6'
  s.add_development_dependency 'rspec'
end
