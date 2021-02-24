# frozen_string_literal: true

lib = File.expand_path('lib', __dir__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'pg_column_byte_packer/version'

Gem::Specification.new do |spec|
  spec.name          = 'pg_column_byte_packer'
  spec.version       = PgColumnBytePacker::VERSION
  spec.authors       = [
    'jcoleman'
  ]
  spec.email         = ['code@getbraintree.com']

  spec.summary       = 'Auto-order table columns for optimize disk space usage'
  spec.homepage      = 'https://github.com/braintree/pg_column_byte_packer'
  spec.license       = 'MIT'

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  spec.files         = Dir.chdir(File.expand_path(__dir__)) do
    `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  end
  spec.bindir        = 'exe'
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ['lib']

  spec.add_development_dependency 'appraisal', '>= 2.2.0'
  spec.add_development_dependency 'db-query-matchers', '>= 0.9.0'
  spec.add_development_dependency 'pry'
  spec.add_development_dependency 'pry-byebug'
  spec.add_development_dependency 'rake', '~> 10.0'
  spec.add_development_dependency 'relation_to_struct'
  spec.add_development_dependency 'rspec', '~> 3.0'

  spec.add_dependency 'activerecord', '>= 5.2'
  spec.add_dependency 'pg'
  spec.add_dependency 'pg_query'
end
