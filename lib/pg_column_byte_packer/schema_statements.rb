require "active_record"
require "active_record/migration"

module PgColumnBytePacker
  module SchemaCreation
    def self.sql_type_alignment_cache
      @@sql_type_alignment_cache ||= {}
    end

    def visit_TableDefinition(o)
      columns_hash = o.instance_variable_get(:@columns_hash)

      sorted_column_tuples = columns_hash.sort_by do |name, col|
        sql_type = type_to_sql(
          col.type,
          :limit => col.limit,
          :precision => col.precision,
          :scale => col.scale,
          :primary_key => col.primary_key?,
        )

        alignment = PgColumnBytePacker::SchemaCreation.sql_type_alignment_cache[sql_type] ||=
          case sql_type
          when "bigint", "timestamp", /\Abigserial( primary key)?/
            8 # Actual alignment for these types.
          when "integer", "date", /\Aserial( primary key)?/
            4 # Actual alignment for these types.
          when "bytea"
            # These types generally have an alignment of 4, but values of at most 127 bytes
            # long they are optimized into 2 byte alignment.
            # Since we'd expect any binary fields to be relatively long, we'll assume they
            # won't fit into the optimized case.
            4
          when "text", "citext", "character varying"
            # These types generally have an alignment of 4, but values of at most 127 bytes
            # long they are optimized into 2 byte alignment.
            # Since we don't have a good heuristic for determining which columns are likely
            # to be long or short, we currently just slot them all after the columns we
            # believe will always be long.
            # If desired we could also differentiate on length limits if set.
            3
          when /\Acharacter varying\(\d+\)/
            if (limit = /\Acharacter varying\((\d+)\)/.match(sql_type)[1])
              if limit.to_i <= 127
                2
              else
                4
              end
            end
          when "smallint", "boolean"
            2 # Actual alignment for these types.
          else
            if @conn.select_value("SELECT typtype FROM pg_type WHERE typname = '#{sql_type}'") == "e"
              4
            elsif (typalign = @conn.select_value("SELECT typalign FROM pg_type WHERE typname = '#{sql_type}'"))
              case typalign
              when "c"
                0
              when "s"
                2
              when "i"
                4
              when "d"
                8
              end
            else
              0
            end
          end

        # Ordering components in order of most importance to least importance.
        [
          -alignment, # Sort alignment descending.
          col.options[:primary_key] ? 0 : 1, # Sort PRIMARY KEY first.
          (case col.null when nil, true then 1 else 0 end), # Sort NOT NULL first.
          col.default && (col.null || col.null.nil?) ? 0 : 1, # Sort DEFAULT first (but only when also nullable).
          name, # Sort name ascending.
        ]
      end

      columns_hash.clear
      sorted_column_tuples.each do |(name, column)|
        columns_hash[name] = column
      end

      # binding.pry
      super(o)
    end
  end
end

ActiveRecord::ConnectionAdapters::AbstractAdapter::SchemaCreation.prepend(PgColumnBytePacker::SchemaCreation)
