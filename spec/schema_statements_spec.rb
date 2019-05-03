RSpec.describe PgColumnBytePacker::SchemaCreation do
  def with_connection(config, &block)
    ActiveRecord::Base.establish_connection(config)
    # spec = ActiveRecord::ConnectionAdapters::ConnectionSpecification::Resolver.new({}).spec(config)
    # connection_pool = ActiveRecord::ConnectionAdapters::ConnectionPool.new(spec)
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

  let(:migration_class) { ActiveRecord::Migration::Current }

  describe "#create_table" do
    around(:each) do |example|
      run_on_fresh_database do
        ActiveRecord::Base.connection.execute("CREATE EXTENSION citext")
        migration.suppress_messages do
          example.run
        end
      end
    end

    let(:migration) { Class.new(migration_class) }
    let(:alphabet) { ("a".."z").to_a }
    def random_word
      15.times.map { alphabet.sample }.join
    end

    it "orders timestamps along with int8" do
      migration.create_table(:tests, :id => false) do |t|
        t.timestamp :d_timestamp
        t.integer :b_int4, :limit => 8
        t.datetime :a_timestamp
        t.integer :c_int4, :limit => 8
      end

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_timestamp", "b_int4", "c_int4", "d_timestamp"])
    end

    it "orders an int8 primary key at the beginning of the 8-byte alignment group" do
      migration.create_table(:tests) do |t|
        t.integer :a_int8, :limit => 8, :null => false
        t.integer :z_int8, :limit => 8, :null => false
        t.timestamps :null => false
      end

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["id", "a_int8", "created_at", "updated_at", "z_int8"])
    end

    it "orders serials along with int8" do
      migration.create_table(:tests, :id => false) do |t|
        t.bigserial :d_serial
        t.integer :b_int8, :limit => 8
        t.bigserial :a_serial
        t.integer :c_int8, :limit => 8
      end

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_serial", "b_int8", "c_int8", "d_serial"])
    end

    it "orders int4 after int8" do
      migration.create_table(:tests, :id => false) do |t|
        t.integer :a_int4, :limit => 4
        t.integer :b_int8, :limit => 8
        t.integer :c_int8, :limit => 8
        t.integer :d_int4, :limit => 4
      end

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_int8", "c_int8", "a_int4", "d_int4"])
    end

    it "orders enums after int8" do
      enum_type_name = random_word
      migration.execute("CREATE TYPE #{enum_type_name} AS ENUM ()")
      migration.create_table(:tests, :id => false) do |t|
        t.column :a_enum, enum_type_name
        t.integer :b_int8, :limit => 8
        t.integer :c_int8, :limit => 8
        t.column :d_enum, enum_type_name
      end

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_int8", "c_int8", "a_enum", "d_enum"])
    end

    it "orders an int4 primary key at the beginning of the 4-byte alignment group" do
      migration.create_table(:tests, :id => false) do |t|
        t.integer :a_int4, :limit => 4, :null => false
        t.integer :b_int8, :limit => 8, :null => false
        t.integer :id, :limit => 4, :primary_key => true
        t.integer :z_int4, :limit => 4, :null => false
      end

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_int8", "id", "a_int4", "z_int4"])
    end

    it "orders enums along with int4" do
      enum_type_name = random_word
      migration.execute("CREATE TYPE #{enum_type_name} AS ENUM ()")
      migration.create_table(:tests, :id => false) do |t|
        t.column :d_enum, enum_type_name
        t.integer :b_int4, :limit => 4
        t.column :a_enum, enum_type_name
        t.integer :c_int4, :limit => 4
      end

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_enum", "b_int4", "c_int4", "d_enum"])
    end

    it "only looks up enums in pg_type once" do
      enum_type_name = random_word
      migration.execute("CREATE TYPE #{enum_type_name} AS ENUM ()")

      expect do
        migration.create_table(:tests, :id => false) do |t|
          t.column :d_enum, enum_type_name
        end
        migration.create_table(:other_tests, :id => false) do |t|
          t.column :d_enum, enum_type_name
        end
      end.to make_database_queries(matching: /pg_type/, count: 1)
    end

    it "orders dates after int8" do
      migration.create_table(:tests, :id => false) do |t|
        t.date :a_date
        t.integer :b_int8, :limit => 8
        t.integer :c_int8, :limit => 8
        t.date :d_date
      end

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_int8", "c_int8", "a_date", "d_date"])
    end

    it "orders dates along with int4" do
      migration.create_table(:tests, :id => false) do |t|
        t.date :d_date
        t.integer :b_int4, :limit => 4
        t.date :a_date
        t.integer :c_int4, :limit => 4
      end

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_date", "b_int4", "c_int4", "d_date"])
    end

    it "orders byteas after int8" do
      migration.create_table(:tests, :id => false) do |t|
        t.binary :a_bytea
        t.integer :b_int8, :limit => 8
        t.integer :c_int8, :limit => 8
        t.binary :d_bytea
      end

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_int8", "c_int8", "a_bytea", "d_bytea"])
    end

    it "orders byteas along with int4" do
      migration.create_table(:tests, :id => false) do |t|
        t.binary :d_bytea
        t.integer :b_int4, :limit => 4
        t.binary :a_bytea
        t.integer :c_int4, :limit => 4
      end

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_bytea", "b_int4", "c_int4", "d_bytea"])
    end

    it "orders decimals after int8" do
      migration.create_table(:tests, :id => false) do |t|
        t.binary :a_decimal
        t.integer :b_int8, :limit => 8
        t.integer :c_int8, :limit => 8
        t.binary :d_decimal
      end

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_int8", "c_int8", "a_decimal", "d_decimal"])
    end

    it "orders decimals along with int4" do
      migration.create_table(:tests, :id => false) do |t|
        t.binary :d_decimal
        t.integer :b_int4, :limit => 4
        t.binary :a_decimal
        t.integer :c_int4, :limit => 4
      end

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_decimal", "b_int4", "c_int4", "d_decimal"])
    end

    it "orders serials after int8" do
      migration.create_table(:tests, :id => false) do |t|
        t.serial :a_serial, :limit => 4
        t.integer :b_int8, :limit => 8
        t.integer :c_int8, :limit => 8
        t.serial :d_serial, :limit => 4
      end

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_int8", "c_int8", "a_serial", "d_serial"])
    end

    it "orders serials along with int4" do
      migration.create_table(:tests, :id => false) do |t|
        t.serial:d_serial, :limit => 4
        t.integer :b_int4, :limit => 4
        t.serial :a_serial, :limit => 4
        t.integer :c_int4, :limit => 4
      end

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_serial", "b_int4", "c_int4", "d_serial"])
    end

    it "orders text after int4" do
      migration.create_table(:tests, :id => false) do |t|
        t.citext :a_text
        t.integer :b_int4, :limit => 4
        t.integer :c_int4, :limit => 4
        t.citext :d_text
      end

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_int4", "c_int4", "a_text", "d_text"])
    end

    it "orders citext after int4" do
      migration.create_table(:tests, :id => false) do |t|
        t.citext :a_citext
        t.integer :b_int4, :limit => 4
        t.integer :c_int4, :limit => 4
        t.citext :d_citext
      end

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_int4", "c_int4", "a_citext", "d_citext"])
    end

    it "orders text along with citext" do
      migration.create_table(:tests, :id => false) do |t|
        t.text :d_text
        t.citext :b_citext
        t.text :a_text
        t.citext :c_citext
      end

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_text", "b_citext", "c_citext", "d_text"])
    end

    it "orders varchar with length > 127 along with int4" do
      migration.create_table(:tests, :id => false) do |t|
        t.integer :d_integer, :limit => 4
        t.string :b_varchar, :limit => 255
        t.integer :a_integer, :limit => 4
        t.string :c_varchar, :limit => 255
      end

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_integer", "b_varchar", "c_varchar", "d_integer"])
    end

    it "orders varchar with length <= 127 after int4" do
      migration.create_table(:tests, :id => false) do |t|
        t.string :a_varchar, :limit => 127
        t.integer :b_int4, :limit => 4
        t.integer :c_int4, :limit => 4
        t.string :d_varchar, :limit => 127
      end

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_int4", "c_int4", "a_varchar", "d_varchar"])
    end

    it "orders varchar with indeterminate length after int4" do
      migration.create_table(:tests, :id => false) do |t|
        t.string :a_varchar
        t.integer :b_int4, :limit => 4
        t.integer :c_int4, :limit => 4
        t.string :d_varchar
      end

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_int4", "c_int4", "a_varchar", "d_varchar"])
    end

    it "orders text along with varchar with indeterminate length" do
      migration.create_table(:tests, :id => false) do |t|
        t.text :d_text
        t.string :b_varchar
        t.text :a_text
        t.string :c_varchar
      end

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_text", "b_varchar", "c_varchar", "d_text"])
    end

    it "orders smallint after text" do
      migration.create_table(:tests, :id => false) do |t|
        t.integer :a_smallint, :limit => 2
        t.text :b_text
        t.text :c_text
        t.integer :d_smallint, :limit => 2
      end

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_text", "c_text", "a_smallint", "d_smallint"])
    end

    it "orders boolean after text" do
      migration.create_table(:tests, :id => false) do |t|
        t.boolean :a_boolean
        t.text :b_text
        t.text :c_text
        t.boolean :d_boolean
      end

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_text", "c_text", "a_boolean", "d_boolean"])
    end

    it "orders boolean along with smallint" do
      migration.create_table(:tests, :id => false) do |t|
        t.boolean :d_boolean
        t.integer :b_smallint, :limit => 2
        t.boolean :a_boolean
        t.integer :c_smallint, :limit => 2
      end

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a_boolean", "b_smallint", "c_smallint", "d_boolean"])
    end

    it "orders by name for multiple fields of the same type" do
      migration.create_table(:tests, :id => false) do |t|
        t.boolean :a
        t.boolean :c
        t.boolean :b
      end

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a", "b", "c"])
    end

    it "orders NOT NULL columns before nullable columns of the same alignment" do
      migration.create_table(:tests, :id => false) do |t|
        t.integer :a
        t.integer :c
        t.integer :b, :null => false
      end

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b", "a", "c"])
    end

    it "orders columns with a default after NOT NULL columns but before nullable columns of the same alignment" do
      migration.create_table(:tests, :id => false) do |t|
        t.text :a_text
        t.text :c_text, :default => -> { "5" }
        t.text :b_text, :null => false
        t.integer :a_int
        t.integer :c_int, :default => 5
        t.integer :b_int, :null => false
      end

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["b_int", "c_int", "a_int", "b_text", "c_text", "a_text"])
    end

    it "doesn't make 3 ordering groups of NOT NULL, NOT NULL with DEFAULT, and nullable with DEFAULT" do
      # This is just to reduce noise in diffs and make the rules simpler to understand.
      migration.create_table(:tests, :id => false) do |t|
        t.integer :a, :null => false
        t.integer :c, :default => 4
        t.integer :b, :null => false, :default => 5
      end

      ordered_columns = column_order_from_postgresql(table: "tests")
      expect(ordered_columns).to eq(["a", "b", "c"])

    end
  end
end
