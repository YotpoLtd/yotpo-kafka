lib = File.expand_path('lib', __dir__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)


version_suffix = ENV['GEM_VERSION_SUFFIX'] || ''
Gem::Specification.new do |s|
  s.name = 'yotpo-ruby-kafka'
  s.version = '3.1.0'.concat(version_suffix)
  s.date = '2020-12-30'
  s.authors = 'Gophers'
  s.summary = 'yotpo-kafka: encapsulate ruby-kafka library with consume retry mechanism'
  s.files = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end
  s.bindir = 'exe'
  s.executables   = s.files.grep(%r{^exe/}) { |f| File.basename(f) }
  s.require_paths = ['lib']

  s.add_dependency 'avro_turf'
  s.add_dependency 'rake'
  s.add_dependency 'rest-client'
  s.add_dependency 'ruby-kafka', '0.7.6'
  s.add_development_dependency 'rspec'
end
