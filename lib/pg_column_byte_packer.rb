require "pg_column_byte_packer/version"

module PgColumnBytePacker
  def self.sql_type_alignment_cache
    @@sql_type_alignment_cache ||= {}
  end

  def self.ordering_key_for_column(connection:, name:, sql_type:, primary_key:, nullable:, has_default:)
    alignment = PgColumnBytePacker.sql_type_alignment_cache[sql_type] ||=
      case sql_type
      when "bigint", "timestamp", /\Abigserial( primary key)?/
        8 # Actual alignment for these types.
      when "integer", "date", "decimal", "float", /\Aserial( primary key)?/
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
        typtype, typalign = connection.select_rows("SELECT typtype, typalign FROM pg_type WHERE typname = '#{connection.quote_string(sql_type)}'", "Type Lookup").first
        if typtype == "e"
          4
        else
          case typalign
          when "c"
            0
          when "s"
            2
          when "i"
            4
          when "d"
            8
          else
            0
          end
        end
      end

    # Ordering components in order of most importance to least importance.
    [
      -alignment, # Sort alignment descending.
      primary_key ? 0 : 1, # Sort PRIMARY KEY first.
      nullable ? 1 : 0, # Sort NOT NULL first.
      has_default && nullable ? 0 : 1, # Sort DEFAULT first (but only when also nullable).
      name, # Sort name ascending.
    ]
  end
end

require "pg_column_byte_packer/schema_statements"
