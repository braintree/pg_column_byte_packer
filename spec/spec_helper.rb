require "bundler/setup"
require "logger"
require "pg_column_byte_packer"
require "relation_to_struct"
require "db-query-matchers"
require "tempfile"
require "pry"
require_relative "postgresql_spec_helpers"

RSpec.configure do |config|
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = ".rspec_status"

  # Disable RSpec exposing methods globally on `Module` and `main`
  config.disable_monkey_patching!

  # config.use_transactional_fixtures = false

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end

  config.before(:all) do
    server_version = ActiveRecord::Base.connection.select_value("SHOW server_version")
    puts "DEBUG: Connecting to Postgres server version #{server_version}"
    pg_dump_version = `pg_dump --version`
    puts "DEBUG: Using pg_dump version #{pg_dump_version}"
  end

  config.before(:each) do
    if PgColumnBytePacker.class_variables.include?(:@@sql_type_alignment_cache)
      PgColumnBytePacker.remove_class_variable(:@@sql_type_alignment_cache)
    end
  end

  config.include(PostgresqlSpecHelpers)
end

ActiveRecord::Base.configurations = {
  "test" => {
    "adapter" => "postgresql",
    "host" => ENV["PGHOST"] || "localhost",
    "port" => ENV["PGPORT"] || "5432",
    "database" => "pg_column_byte_packer_tests",
    "encoding" => "utf8",
    "username" => ENV["PGUSER"] || "postgres",
    "password" => ENV["PGPASSWORD"] || "postgres",
    "prepared_statements" => false,
  },
}

config =
  if ActiveRecord::VERSION::MAJOR < 7
    ActiveRecord::Base.configurations["test"]
  else
    ActiveRecord::Base.configurations.configs_for(env_name: "test").first
  end

# Avoid having to require Rails when the task references `Rails.env`.
ActiveRecord::Tasks::DatabaseTasks.instance_variable_set('@env', "test")

ActiveRecord::Tasks::DatabaseTasks.drop_current
ActiveRecord::Tasks::DatabaseTasks.create_current
ActiveRecord::Base.establish_connection(config)
