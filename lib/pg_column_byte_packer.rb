require "pg_column_byte_packer/version"
require "pg_query"

module PgColumnBytePacker
  def self.sql_type_alignment_cache
    @@sql_type_alignment_cache ||= {}
  end

  def self.ordering_key_for_column(connection:, name:, sql_type:, type_schema: nil, primary_key:, nullable:, has_default:)
    alignment = PgColumnBytePacker.sql_type_alignment_cache[sql_type] ||= (
      if type_schema.nil?
        fake_query = "CREATE TABLE t(c #{sql_type});"
        parsed = begin
           PgQuery.parse(fake_query)
         rescue PgQuery::ParseError
           nil
         end

        if parsed && (column_def = parsed.tree[0]["RawStmt"]["stmt"]["CreateStmt"]["tableElts"][0]["ColumnDef"])
          type_identifiers = column_def["typeName"]["TypeName"]["names"].map { |s| s["String"]["str"] }
          case type_identifiers.size
          when 1
            # Do nothing; we already have a bare type.
          when 2
            type_schema, bare_type = type_identifiers
          else
            raise ArgumentError, "Unexpected number of identifiers in type declaration for column: `#{name}`, identifiers: #{type_identifiers.inspect}"
          end
        end
      end

      if type_schema == "pg_catalog"
        sql_type = case sql_type
        when "char", "\"char\""
          # The SQL standard defines bare 'character' as a single character
          # type. The quoted variant is also single character, but raises
          # an error on inputs longer than a single character (unlike the
          # which requires it to be silently trucated).
          #
          # We don't yet use the length for the character type, but it seems
          # worth retaining it here for completeness (rather than solely
          # handling the quoted "char" case, which is what we have to have
          # for things to work minimally).
          #
          # See: https://www.postgresql.org/docs/current/datatype-character.html
          #      and format_type.c
          "character(1)"
        when "bit"
          # The SQL standard defines bare 'bit' as a single bit type.
          #
          # See: https://www.postgresql.org/docs/current/datatype-bit.html
          "bit(1)"
        when "\"bit\""
          # The quoted variant, instead of being parallel to the quoted
          # variant of char, allows any number of bits, but is technically
          # a different entry in pg_type than varchar.
          "bit"
        else
          sql_type
        end
      end

      bare_type = if type_schema
        if sql_type.start_with?("#{type_schema}.")
          sql_type.sub("#{type_schema}.", "")
        elsif sql_type.start_with?("\"#{type_schema}\".")
          sql_type.sub("\"#{type_schema}\".", "")
        else
          sql_type
        end
      else
        sql_type
      end

      # Ignore array designations. This seems like something we could
      # so with the parsing above, and we could, and, in fact, that
      # would also almost certainly allow us to rip out most of the
      # ActiveRecord generated alias type name matching below, but
      # it would also mean a more thorough refactor below for types
      # with size designations (e.g., when ActiveRecord generates
      # "float(23)"). So we use this simple regex cleanup for now.
      bare_type = bare_type.sub(/(\[\])+\Z/, "")

      # Sort out the alignment. Most of the type name matching is to
      # support the naming variants that ActiveRecord generates (often
      # they're aliases, like "integer", for which PostgreSQL internally
      # has a different canonical name: "int4"). There are also
      # a few cases we have to handle where the output from pgdump
      # doesn't match the canonical name in pg_type; e.g., "float8" is
      # canonical, but pgdump outputs "double precision".
      case bare_type
      when "bigint", "double precision", /\Atimestamp(tz| with time zone| without time zone)?/, /\Abigserial( primary key)?/
        8 # Actual alignment for these types.
      when "integer", "date", /\Atime(tz| with time zone| without time zone)?/, /\Adecimal(\([^\)]+\))?/, "real", /\Aserial( primary key)?/
        # Note: unlike the others which always take a fixed amount of space,
        # the numeric/decimal type is stored in a variable amount of space (see:
        # https://www.postgresql.org/docs/10/datatype-numeric.html) but pg_type
        # shows that its alignment is the same as integer. Postgres canonicalizes
        # this type to numeric, but we have to still support the decimal
        # designation for ActiveRecord inputs.

        4 # Actual alignment for these types.
      when "bytea"
        # These types generally have an alignment of 4, but values of at most 127 bytes
        # long they are optimized into 2 byte alignment.
        # Since we'd expect any binary fields to be relatively long, we'll assume they
        # won't fit into the optimized case.
        4
      when "text", "citext", "character varying", "bit varying", "bit"
        # These types generally have an alignment of 4 (as designated by pg_type
        # having a typalign value of 'i', but they're special in that small values
        # have an optimized storage layout. Beyond the optimized storage layout, though,
        # these small values also are not required to respect the alignment the type
        # would otherwise have. Specifically, values with a size of at most 127 bytes
        # aren't aligned. That 127 byte cap, however, includes an overhead byte to store
        # the length, and so in reality the max is 126 bytes. Interestingly TOASTable
        # values are also treated that way, but we don't have a good way of knowing which
        # values those will be.
        #
        # See: `fill_val()` in src/backend/access/common/heaptuple.c (in the conditional
        # `else if (att->attlen == -1)` branch.
        #
        # When no limit modifier has been applied we don't have a good heuristic for
        # determining which columns are likely to be long or short, so we currently
        # just slot them all after the columns we believe will always be long.
        3
      when /\Acharacter varying\(\d+\)/
        # However, when a limit modifier has been applied we can make stronger assumptions.
        if (limit = /\Acharacter varying\((\d+)\)/.match(sql_type)[1])
          if limit.to_i <= 126
            # If we know the limit guarantees we'll fit into the unaligned storage
            # optimization, then we can go ahead and treat it as unaligned.
            1
          else
            # If the limit would allow strings that require alignment, then we assume it's
            # more likely to exeed the optimization cap and slot them after the columns
            # we know for certain will require integer alignment.
            3
          end
        end
      when /\Abit varying\(\d+\)/
        # This type is functionally the same as varchar above, but the calculation we need
        # to do has been scaled since the limit is expressed in bits rather than bytes.
        if (limit = /\Abit varying\((\d+)\)/.match(sql_type)[1])
          if limit.to_i <= (126 * 8)
            1
          else
            3
          end
        end
      when /\Afloat(\(\d+\))?/
        precision_match = /\Afloat\((\d+)\)?/.match(sql_type)
        if precision_match
          # Precision here is a number of binary digits;
          # see https://www.postgresql.org/docs/10/datatype-numeric.html
          # for more information.
          if precision_match[1].to_i >= 25
            8 # Double precision
          else
            4 # Real
          end
        else
          8 # Default is double precision
        end
      else
        type_without_modifier, modifier = bare_type.match(/\A([^\(]+)(\([^\)]+\))?/)[1..-1]

        pg_type_typname = case type_without_modifier
        when "boolean"
          "bool"
        when "smallint"
          "int2"
        when "character"
          "char"
        else
          # There are other cases you might expect to see here (for other system
          # types like varchar and varbit  where the external name generated by
          # format_type_extended() in Postgres's format_type.c doesn't match the
          # pg_type typaname), but we've already handled them separately above
          # (since we have additional rules to apply to them).
          type_without_modifier
        end

        typtype, typalign = connection.select_rows(<<~SQL, "Type Lookup").first
          SELECT typ.typtype, typ.typalign
          FROM pg_type typ
          JOIN pg_namespace nsp ON nsp.oid = typ.typnamespace
          WHERE typname = '#{connection.quote_string(pg_type_typname)}'
            #{type_schema ? "AND nsp.nspname = '#{connection.quote_string(type_schema)}'" : ""}
        SQL

        if typtype.nil?
          raise ArgumentError, "Got sql_type: `#{sql_type}` and type_schema: `#{type_schema}` but was unable to find entry in pg_type."
        end

        if typtype == "e"
          4
        else
          case typalign
          when "c"
            # Character types, for example, occupy a variable amount of space
            # (though fixed in the sense that it's specified up front for each
            # column definition) but require no alignment.
            1
          when "s"
            2
          when "i"
            4
          when "d"
            8
          else
            1
          end
        end
      end
    )

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
require "pg_column_byte_packer/pg_dump"
