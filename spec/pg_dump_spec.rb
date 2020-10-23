require "open3"

RSpec.describe PgColumnBytePacker::PgDump do
  def run_shell_command(command, env:)
    stdout, stderr, command_status = Open3.capture3(env, command)

    raise RuntimeError, "Command failed: `#{command}` (status: #{command_status})\nstdout `#{stdout}`\nstderr: `#{stderr}`" unless command_status.exitstatus.zero?
  end

  def pg_dump_env_vars
    config = ActiveRecord::Base.configurations["test"]
    {
      "PGHOST" => config["host"],
      "PGPORT" => config["port"],
      "PGUSER" => config["username"],
      "PGPASSWORD" => config["password"],
    }
  end

  def dump_table_definitions_and_restore_reordered
    config = ActiveRecord::Base.connection.pool.spec.config
    Tempfile.open("structure.sql") do |file|
      run_shell_command("pg_dump --schema-only #{config[:database]} > #{file.path}", env: pg_dump_env_vars)

      # Behavior under test.
      PgColumnBytePacker::PgDump.sort_columns_for_definition_file(
        file.path,
        connection: ActiveRecord::Base.connection
      )

      # Drop all tables so we can re-apply the schema.
      ActiveRecord::Base.connection.tables.each do |table|
        ActiveRecord::Base.connection.execute("DROP TABLE #{table}")
      end
      enum_types = ActiveRecord::Base.connection.select_rows <<~SQL
        SELECT nsp.nspname, typ.typname
        FROM pg_type typ
        JOIN pg_namespace nsp ON nsp.oid = typ.typnamespace
        WHERE typtype = 'e'
      SQL
      enum_types.each do |schema, type|
        conn = ActiveRecord::Base.connection
        conn.execute("DROP TYPE \"#{conn.quote_string(schema)}\".#{type}")
      end

      # Restore.
      run_shell_command("psql -f #{file.path} #{config[:database]}", env: pg_dump_env_vars)
    end
  end

  def dump_table_definitions_reordered
    config = ActiveRecord::Base.connection.pool.spec.config
    Tempfile.open("structure.sql") do |file|
      run_shell_command("pg_dump --schema-only #{config[:database]} > #{file.path}", env: pg_dump_env_vars)

      # Behavior under test.
      PgColumnBytePacker::PgDump.sort_columns_for_definition_file(
        file.path,
        connection: ActiveRecord::Base.connection
      )

      File.read(file.path)
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

    it "orders int8 arrays along with int8" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          d_int8arr bigint[],
          b_int8 bigint,
          a_int8arr bigint[][],
          c_int8 bigint
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_int8arr", "b_int8", "c_int8", "d_int8arr"])
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

    it "orders doubles along with int8" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          d_float8 double precision,
          b_int8 bigint,
          a_float8 double precision,
          c_int8 bigint
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_float8", "b_int8", "c_int8", "d_float8"])
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

    it "orders int4 arrays along with int4" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          d_int4arr integer[],
          b_int4 int,
          a_int4arr integer[][],
          c_int4 int
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_int4arr", "b_int4", "c_int4", "d_int4arr"])
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

    it "orders time after int8" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          a_time time,
          b_int8 bigint,
          c_int8 bigint,
          d_time time
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_int8", "c_int8", "a_time", "d_time"])
    end

    it "orders time along with int4" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          d_time time,
          b_int4 integer,
          a_time time,
          c_int4 integer
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_time", "b_int4", "c_int4", "d_time"])
    end

    it "orders timetz along with int4" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          d_time timetz,
          b_int4 integer,
          a_time timetz,
          c_int4 integer
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_time", "b_int4", "c_int4", "d_time"])
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

    # Both `numeric` and `decimal` are the same type in Postgres; see:
    # https://www.postgresql.org/message-id/20211.1325269672@sss.pgh.pa.us
    it "orders numeric after int8" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          a_numeric numeric,
          b_int8 bigint,
          c_int8 bigint,
          d_numeric numeric
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_int8", "c_int8", "a_numeric", "d_numeric"])
    end

    it "orders numeric along with int4" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          d_numeric numeric,
          b_int4 integer,
          a_numeric numeric,
          c_int4 integer
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_numeric", "b_int4", "c_int4", "d_numeric"])
    end

    it "supports numeric with a modifier" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          d_numeric numeric(1),
          b_int4 integer,
          a_numeric numeric(1),
          c_int4 integer
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_numeric", "b_int4", "c_int4", "d_numeric"])
    end

    it "orders reals along with int4" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          d_float4 real,
          b_int4 integer,
          a_float4 real,
          c_int4 integer
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_float4", "b_int4", "c_int4", "d_float4"])
    end

    it "orders serials after int8" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          a_serial serial,
          b_int8 bigint not null,
          c_int8 bigint not null,
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

    it "orders varbit with length > (8 * 127) along with int4" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          d_integer integer,
          b_varbit varbit(#{8 * 127 + 1}),
          a_integer integer,
          c_varbit varbit(#{8 * 127 + 1})
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_integer", "b_varbit", "c_varbit", "d_integer"])
    end

    it "orders varbit with length <= (8 * 127) after int4" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          a_varbit varbit(#{8 * 127}),
          b_int4 integer,
          c_int4 integer,
          d_varbit varbit(#{8 * 127})
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_int4", "c_int4", "a_varbit", "d_varbit"])
    end

    it "orders varbit with indeterminate length after int4" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          a_varbit varbit,
          b_int4 integer,
          c_int4 integer,
          d_varbit varbit
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_int4", "c_int4", "a_varbit", "d_varbit"])
    end

    it "orders text along with varbit with indeterminate length" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          d_text text,
          b_varbit varbit,
          a_text text,
          c_varbit varbit
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_text", "b_varbit", "c_varbit", "d_text"])
    end

    it "orders \"bit\" along with varbit with indeterminate length" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          d_bit "bit",
          b_varbit varbit,
          a_bit "bit",
          c_varbit varbit
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_bit", "b_varbit", "c_varbit", "d_bit"])
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

    it "orders char after smallint" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          a_char character,
          b_smallint smallint,
          c_smallint smallint,
          d_char character
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_smallint", "c_smallint", "a_char", "d_char"])
    end

    it "orders char(n) with char" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          d_char char(1),
          b_char char,
          a_char char(130),
          c_char char
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_char", "b_char", "c_char", "d_char"])
    end

    it "orders \"char\" with char" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          d_char "char",
          b_char char,
          a_char "char",
          c_char char
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_char", "b_char", "c_char", "d_char"])
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

    it "handles quoted column names (but without spaces)" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          -- Using a keyword will make pgdump have to quote
          -- the identifier in its SQL output.
          "limit" integer
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["limit"])
    end

    it "handles quoted column names (with spaces)" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          "my column" integer
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["my column"])
    end

    it "puts constraints at the end of the statement" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          i integer,
          CONSTRAINT tests_i_is_five CHECK (i = 5)
        )
      SQL

      contents = dump_table_definitions_reordered()

      expected_statement = <<~SQL
      CREATE TABLE public.tests (
          i integer,
          CONSTRAINT tests_i_is_five CHECK ((i = 5))
      );
      SQL

      expect(contents).to include(expected_statement)
    end

    it "properly handles table config modifiers" do
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE TABLE tests (
          i integer
        ) WITH (autovacuum_analyze_scale_factor='0.002');
      SQL

      contents = dump_table_definitions_reordered()

      expected_statement = <<~SQL
      CREATE TABLE public.tests (
          i integer
      )
      WITH (autovacuum_analyze_scale_factor='0.002');
      SQL

      expect(contents).to include(expected_statement)
    end

    it "handles columns with a types from a different schema" do
      enum_type_name = random_word
      ActiveRecord::Base.connection.execute <<~SQL
        CREATE SCHEMA "my schema";
        CREATE SCHEMA myschema;
        CREATE TYPE "my schema".#{enum_type_name} AS ENUM ();
        CREATE TYPE myschema.#{enum_type_name} AS ENUM ();
        CREATE TABLE tests (
          a_bool boolean,
          b_enum myschema.#{enum_type_name},
          c_bool boolean,
          d_enum "my schema".#{enum_type_name}
        )
      SQL

      dump_table_definitions_and_restore_reordered()

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_enum", "d_enum", "a_bool", "c_bool"])
    end
  end
end
