Gem::Specification.new do |s|
  s.name = 'ruote-mongodb'
  s.homepage = 'http://github.com/PlasticLizard/ruote-mongodb'
  s.summary = 'MongoDB persistence for Ruote'
  s.require_path = 'lib'
  s.authors = ['Patrick Gannon', 'Nathan Stults']
  s.email = ['hereiam@sonic.net']
  s.version = '0.1.0'
  s.platform = Gem::Platform::RUBY
  s.files = Dir.glob("{lib,test,spec}/**/*") + %w[LICENSE README]

  s.add_dependency 'ruote'
  s.add_dependency 'yajl-ruby'

  s.add_development_dependency 'rspec'
  s.add_development_dependency 'test-unit'


end

