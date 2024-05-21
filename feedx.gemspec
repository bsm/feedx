Gem::Specification.new do |s|
  s.name          = 'feedx'
  s.version       = '0.13.1'
  s.authors       = ['Black Square Media Ltd']
  s.email         = ['info@blacksquaremedia.com']
  s.summary       = %(Exchange data between components via feeds)
  s.description   = %(Use feeds to exchange data between (micro-)services.)
  s.homepage      = 'https://github.com/bsm/feedx'
  s.license       = 'Apache-2.0'

  s.files         = `git ls-files -z`.split("\x0").reject {|f| f.start_with?('spec/') }
  s.require_paths = ['lib']
  s.required_ruby_version = '>= 2.7'

  s.add_dependency 'bfs', '>= 0.8.0'

  s.add_development_dependency 'bundler'
  s.add_development_dependency 'pbio'
  s.add_development_dependency 'rake'
  s.add_development_dependency 'rspec'
  s.add_development_dependency 'rubocop-bsm'
  s.metadata['rubygems_mfa_required'] = 'true'
end
