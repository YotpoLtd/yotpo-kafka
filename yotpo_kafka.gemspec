lib = File.expand_path('lib', __dir__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |s|
  s.name = 'yotpo-ruby-kafka'
  s.version = '3.0.5'
  s.date = '2021-03-10'
  s.authors = 'Gophers'
  s.summary = 'yotpo-kafka: encapsulate ruby-kafka library with consume retry mechanism'
  s.files = `git ls-files -z`.split("\x0").reject do |f|
    f.match(%r{^(test|spec|features)/})
  end
  s.bindir = 'exe'
  s.executables   = s.files.grep(%r{^exe/}) { |f| File.basename(f) }
  s.require_paths = ['lib']

  s.add_dependency 'rake'
  s.add_dependency 'rest-client'
  s.add_dependency 'ruby-kafka', '0.7.6'
  s.add_development_dependency 'rspec'
end
