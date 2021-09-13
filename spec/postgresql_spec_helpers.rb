module PostgresqlSpecHelpers
  def with_connection(config, &block)
    ActiveRecord::Base.establish_connection(config)
    begin
      block.call
    ensure
      ActiveRecord::Base.connection.pool.disconnect!
    end
  end

  def test_configuration
    if ActiveRecord.gem_version >= Gem::Version.new("6.1.0")
      ActiveRecord::Base.configurations.configs_for(env_name: "test").first.configuration_hash.symbolize_keys
    else
      ActiveRecord::Base.configurations["test"].symbolize_keys
    end
  end

  def run_on_fresh_database(&block)
    config = test_configuration

    config = config.merge(
      :database => "#{config[:database]}_#{Process.pid}",
    )

    begin
      with_connection(config.merge(:database => "postgres")) do
        ActiveRecord::Base.connection.create_database(config[:database], config)
      end

      with_connection(config) do
        block.call
      end
    ensure
      with_connection(config.merge(:database => "postgres")) do
        ActiveRecord::Base.connection.drop_database(config[:database])
      end
    end
  end

  def column_order_from_postgresql(table:)
    ActiveRecord::Base.pluck_from_sql <<-SQL
      SELECT attname
      FROM pg_attribute
      WHERE attrelid = (
        SELECT oid
        FROM pg_class
        WHERE pg_class.relname = '#{table}'
          AND pg_class.relnamespace = (
            SELECT pg_namespace.oid
            FROM pg_namespace
            WHERE pg_namespace.nspname = 'public'
          )
      ) AND attnum >= 0
      ORDER BY attnum
    SQL
  end

  def type_name_from_postgresql(table:, column:)
    ActiveRecord::Base.value_from_sql <<-SQL
      SELECT pg_catalog.format_type(atttypid, atttypmod)
      FROM pg_attribute
      WHERE attrelid = (
        SELECT oid
        FROM pg_class
        WHERE pg_class.relname = '#{table}'
          AND pg_class.relnamespace = (
            SELECT pg_namespace.oid
            FROM pg_namespace
            WHERE pg_namespace.nspname = 'public'
          )
      ) AND attnum >= 0 AND attname = '#{column}'
    SQL
  end
end
