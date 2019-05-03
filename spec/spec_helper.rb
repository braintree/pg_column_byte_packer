require "bundler/setup"
require "pg_column_byte_packer"
require "relation_to_struct"
require "db-query-matchers"

ActiveRecord::Base.configurations = {
  "postgresql" => {
    "adapter" => 'postgresql',
    "host" => ENV["PG_HOST"] || 'localhost',
    "port" => ENV["PG_PORT"] || '5432',
    "database" => 'pg_column_byte_packer_tests',
    "encoding" => 'utf8',
    "username" => ENV["PG_USER"] || `whoami`.strip,
    "password" => ENV["PG_PASSWORD"],
  },
}

RSpec.configure do |config|
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = ".rspec_status"

  # Disable RSpec exposing methods globally on `Module` and `main`
  config.disable_monkey_patching!

  # config.use_transactional_fixtures = false

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end
end
