require "active_record"
require "active_record/migration"
require "active_record/connection_adapters/abstract/schema_creation"

module PgColumnBytePacker
  module SchemaCreation
    def visit_TableDefinition(o)
      columns_hash = o.instance_variable_get(:@columns_hash)

      sorted_column_tuples = columns_hash.sort_by do |name, col|
        sql_type = type_to_sql(
          col.type,
          :limit => col.limit,
          :precision => col.precision,
          :scale => col.scale,
          :primary_key => col.primary_key?,
          :enum_type => col.options[:enum_type],
        )

        nullable = if sql_type.match(/\A(big)?serial( primary key)?/)
          col.null == true
        else
          col.null.nil? || col.null == true
        end

        PgColumnBytePacker.ordering_key_for_column(
          connection: @conn,
          name: name,
          sql_type: sql_type,
          primary_key: col.options[:primary_key],
          nullable: nullable,
          has_default: !col.default.nil?
        )
      end

      columns_hash.clear
      sorted_column_tuples.each do |(name, column)|
        columns_hash[name] = column
      end

      super(o)
    end
  end
end

if ActiveRecord.gem_version >= Gem::Version.new("6.1.0")
  ActiveRecord::ConnectionAdapters::SchemaCreation.prepend(PgColumnBytePacker::SchemaCreation)
else
  ActiveRecord::ConnectionAdapters::AbstractAdapter::SchemaCreation.prepend(PgColumnBytePacker::SchemaCreation)
end
