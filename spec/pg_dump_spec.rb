RSpec.describe PgColumnBytePacker::PgDump do
  def with_connection(config, &block)
    ActiveRecord::Base.establish_connection(config)
    begin
      block.call
    ensure
      ActiveRecord::Base.connection.pool.disconnect!
    end
  end

  def run_on_fresh_database(&block)
    config = ActiveRecord::Base.configurations["postgresql"]

    config = config.merge(
      "database" => "#{config["database"]}_#{Process.pid}",
    )

    begin
      with_connection(config.merge("database" => "postgres")) do
        ActiveRecord::Base.connection.create_database(config["database"], config)
      end

      with_connection(config) do
        block.call
      end
    ensure
      with_connection(config.merge("database" => "postgres")) do
        ActiveRecord::Base.connection.drop_database(config["database"])
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

  def dump_table_definitions_and_restore_reordered
    config = ActiveRecord::Base.connection.pool.spec.config
    Tempfile.open("structure.sql") do |file|
      `pg_dump --schema-only #{config[:database]} > #{file.path}`

      # Behavior under test.
      PgColumnBytePacker::PgDump.sort_columns_for_definition_file(
        file.path,
        connection: ActiveRecord::Base.connection
      )

      # Drop all tables so we can re-apply the schema.
      ActiveRecord::Base.connection.tables.each do |table|
        ActiveRecord::Base.connection.execute("DROP TABLE #{table}")
      end
      ActiveRecord::Base.connection.select_values("SELECT typname FROM pg_type WHERE typtype = 'e'").each do |enum_type|
        ActiveRecord::Base.connection.execute("DROP TYPE #{enum_type}")
      end

      # Restore.
      `psql -f #{file.path} #{config[:database]}`
    end
  end

  describe "#create_table" do
    around(:each) do |example|
      run_on_fresh_database do
        ActiveRecord::Base.connection.execute("CREATE EXTENSION citext")
        example.run
      end
    end

    let(:alphabet) { ("a".."z").to_a }
    def random_word
      15.times.map { alphabet.sample }.join
    end

    it "orders timestamps along with int8" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          d_timestamp timestamp,
          b_int8 bigint,
          a_timestamp timestamp,
          c_int8 bigint
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_timestamp", "b_int8", "c_int8", "d_timestamp"])
    end

    it "orders an int8 primary key at the beginning of the 8-byte alignment group" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          a_int8 bigint not null,
          z_int8 bigint not null,
          created_at timestamp not null,
          updated_at timestamp not null,
          id bigserial primary key
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["id", "a_int8", "created_at", "updated_at", "z_int8"])
    end

    it "orders bigserials along with int8" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          d_bigserial bigserial,
          b_int8 bigint not null,
          a_bigserial bigserial,
          c_int8 bigint not null
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_bigserial", "b_int8", "c_int8", "d_bigserial"])
    end

    it "orders int4 after int8" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          a_int4 integer,
          b_int8 bigint,
          c_int8 bigint,
          d_int4 integer
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_int8", "c_int8", "a_int4", "d_int4"])
    end

    it "orders enums after int8" do
      enum_type_name = random_word
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TYPE #{enum_type_name} AS ENUM ();
        CREATE TABLE tests (
          a_enum #{enum_type_name},
          b_int8 bigint,
          c_int8 bigint,
          d_enum #{enum_type_name}
        );
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_int8", "c_int8", "a_enum", "d_enum"])
    end

    it "orders an int4 primary key at the beginning of the 4-byte alignment group" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          a_int4 integer not null,
          b_int8 bigint not null,
          id serial primary key,
          z_int4 integer not null
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_int8", "id", "a_int4", "z_int4"])
    end

    it "orders enums along with int4" do
      enum_type_name = random_word
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TYPE #{enum_type_name} AS ENUM ();
        CREATE TABLE tests (
          d_enum #{enum_type_name},
          b_int4 integer,
          a_enum #{enum_type_name},
          c_int4 integer
        );
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_enum", "b_int4", "c_int4", "d_enum"])
    end

    it "only looks up enums in pg_type once" do
      enum_type_name = random_word
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TYPE #{enum_type_name} AS ENUM ();
        CREATE TABLE tests (
          d_enum #{enum_type_name}
        );
        CREATE TABLE other_tests (
          d_enum #{enum_type_name}
        );
      SQL

      expect do
        dump_table_definitions_and_restore_reordered()
      end.to make_database_queries(matching: /typname.+#{enum_type_name}/m, count: 1)
    end

    it "orders dates after int8" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          a_date date,
          b_int8 bigint,
          c_int8 bigint,
          d_date date
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_int8", "c_int8", "a_date", "d_date"])
    end

    it "orders dates along with int4" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          d_date date,
          b_int4 integer,
          a_date date,
          c_int4 integer
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_date", "b_int4", "c_int4", "d_date"])
    end

    it "orders byteas after int8" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          a_bytea bytea,
          b_int8 bigint,
          c_int8 bigint,
          d_bytea bytea
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_int8", "c_int8", "a_bytea", "d_bytea"])
    end

    it "orders byteas along with int4" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          d_bytea bytea,
          b_int4 integer,
          a_bytea bytea,
          c_int4 integer
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_bytea", "b_int4", "c_int4", "d_bytea"])
    end

    it "orders decimals after int8" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          a_decimal decimal,
          b_int8 bigint,
          c_int8 bigint,
          d_decimal decimal
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_int8", "c_int8", "a_decimal", "d_decimal"])
    end

    it "orders decimals along with int4" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          d_decimal decimal,
          b_int4 integer,
          a_decimal decimal,
          c_int4 integer
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_decimal", "b_int4", "c_int4", "d_decimal"])
    end

    it "orders serials after int8" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          a_serial serial,
          b_int8 bigint,
          c_int8 bigint,
          d_serial serial
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_int8", "c_int8", "a_serial", "d_serial"])
    end

    it "orders serials along with int4" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          d_serial serial,
          b_int4 integer not null,
          a_serial serial,
          c_int4 integer not null
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_serial", "b_int4", "c_int4", "d_serial"])
    end

    it "orders text after int4" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          a_text text,
          b_int4 integer,
          c_int4 integer,
          d_text text
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_int4", "c_int4", "a_text", "d_text"])
    end

    it "orders citext after int4" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          a_citext citext,
          b_int4 integer,
          c_int4 integer,
          d_citext citext
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_int4", "c_int4", "a_citext", "d_citext"])
    end

    it "orders text along with citext" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          d_text text,
          b_citext citext,
          a_text text,
          c_citext citext
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_text", "b_citext", "c_citext", "d_text"])
    end

    it "orders varchar with length > 127 along with int4" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          d_integer integer,
          b_varchar varchar(128),
          a_integer integer,
          c_varchar varchar(128)
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_integer", "b_varchar", "c_varchar", "d_integer"])
    end

    it "orders varchar with length <= 127 after int4" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          a_varchar varchar(127),
          b_int4 integer,
          c_int4 integer,
          d_varchar varchar(127)
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_int4", "c_int4", "a_varchar", "d_varchar"])
    end

    it "orders varchar with indeterminate length after int4" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          a_varchar varchar,
          b_int4 integer,
          c_int4 integer,
          d_varchar varchar
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_int4", "c_int4", "a_varchar", "d_varchar"])
    end

    it "orders text along with varchar with indeterminate length" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          d_text text,
          b_varchar varchar,
          a_text text,
          c_varchar varchar
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_text", "b_varchar", "c_varchar", "d_text"])
    end

    it "orders smallint after text" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          a_smallint smallint,
          b_text text,
          c_text text,
          d_smallint smallint
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_text", "c_text", "a_smallint", "d_smallint"])
    end

    it "orders boolean after text" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          a_boolean boolean,
          b_text text,
          c_text text,
          d_boolean boolean
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_text", "c_text", "a_boolean", "d_boolean"])
    end

    it "orders boolean along with smallint" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          d_boolean boolean,
          b_smallint smallint,
          a_boolean boolean,
          c_smallint smallint
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_boolean", "b_smallint", "c_smallint", "d_boolean"])
    end

    it "orders by name for multiple fields of the same type" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          a boolean,
          c boolean,
          b boolean
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a", "b", "c"])
    end

    it "orders NOT NULL columns before nullable columns of the same alignment" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          a integer,
          c integer,
          b integer not null
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b", "a", "c"])
    end

    it "orders columns with a default after NOT NULL columns but before nullable columns of the same alignment" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          a_text text,
          c_text text default '5',
          b_text text not null,
          a_int integer,
          c_int integer default 5,
          b_int integer not null
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_int", "c_int", "a_int", "b_text", "c_text", "a_text"])
    end

    it "doesn't make 3 ordering groups of NOT NULL, NOT NULL with DEFAULT, and nullable with DEFAULT" do
      # This is just to reduce noise in diffs and make the rules simpler to understand.
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          a integer not null,
          c integer default 4,
          b integer not null default 5
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a", "b", "c"])
    end
  end
end
