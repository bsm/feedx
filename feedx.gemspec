Gem::Specification.new do |s|
  s.name          = 'feedx'
  s.version       = '0.14.0'
  s.authors       = ['Black Square Media Ltd']
  s.email         = ['info@blacksquaremedia.com']
  s.summary       = %(Exchange data between components via feeds)
  s.description   = %(Use feeds to exchange data between (micro-)services.)
  s.homepage      = 'https://github.com/bsm/feedx'
  s.license       = 'Apache-2.0'

  s.files         = `git ls-files -z`.split("\x0").reject {|f| f.start_with?('spec/') }
  s.require_paths = ['lib']
  s.required_ruby_version = '>= 3.2'

  s.add_dependency 'bfs', '>= 0.8.0'
  s.metadata['rubygems_mfa_required'] = 'true'
end
