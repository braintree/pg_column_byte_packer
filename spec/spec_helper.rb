require "bundler/setup"
require "pg_column_byte_packer"
require "relation_to_struct"
require "db-query-matchers"
require 'tempfile'

ActiveRecord::Base.configurations = {
  "postgresql" => {
    "adapter" => 'postgresql',
    "host" => ENV["PGHOST"] || 'localhost',
    "port" => ENV["PGPORT"] || '5432',
    "database" => 'pg_column_byte_packer_tests',
    "encoding" => 'utf8',
    "username" => ENV["PGUSER"] || `whoami`.strip,
    "password" => ENV["PGPASSWORD"],
    "prepared_statements" => false,
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

  config.before(:each) do
    if PgColumnBytePacker.class_variables.include?(:@@sql_type_alignment_cache)
      PgColumnBytePacker.remove_class_variable(:@@sql_type_alignment_cache)
    end
  end
end
