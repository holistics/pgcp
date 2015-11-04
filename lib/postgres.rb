require 'active_support'
require 'active_support/core_ext'
require 'pg'

class Postgres
  # Initialize Postgres instance
  #
  # @param [Hash] dbconfig Database config
  def initialize(dbconfig)
    @dbconfig = dbconfig
  end

  def exec(sql, val=[])
    with_connection do |conn|
      conn.exec sql, val

    end
  end

  def drop_table(schema_name, table_name)
    with_connection do |conn|
      if table_exists?(conn, schema_name, table_name)
        fq_table_name = conn.escape_string("#{schema_name}.#{table_name}")

        sql = <<-SQL.strip_heredoc
          DROP TABLE #{fq_table_name}
        SQL

        conn.exec sql
      end
    end
  end

  def create_table(schema_name, table_name, columns, options={})
    with_connection do |conn|
      if options[:temporary]
        table_name = conn.escape_string(table_name)
        create_sql = create_table_statement(conn, columns,
                                            table_name,
                                            options)
        conn.transaction do
          conn.exec create_sql
          conn.exec "DROP TABLE IF EXISTS #{table_name}"
        end

        true
      else
        unless table_exists?(conn, schema_name, table_name)
          create_sql = create_table_statement(conn, columns,
                                              "#{schema_name}.#{table_name}",
                                              options)
          conn.exec create_sql

          true
        end

        false
      end
    end
  end

  def create_table_from_query(query, schema_name, table_name, columns, options={})
    with_connection do |conn|
      create_sql = create_table_statement(conn, columns,
                                          "#{schema_name}.#{table_name}",
                                          options)
      conn.transaction do
        conn.exec "DROP TABLE IF EXISTS #{schema_name}.#{table_name}"
        conn.exec create_sql
        conn.exec "INSERT INTO #{schema_name}.#{table_name}\n#{query}"
      end
    end
  end

  def hotswap_table(schema_name, src_table_name, dst_table_name)
    with_connection do |conn|
      conn.transaction do
        schema_name = conn.escape_string(schema_name)
        dst_table_name = conn.escape_string(dst_table_name)
        conn.exec "DROP TABLE #{schema_name}.#{dst_table_name}" if table_exists?(conn, schema_name, dst_table_name)
        conn.exec "ALTER TABLE #{schema_name}.#{src_table_name} RENAME TO #{dst_table_name}"
        #conn.exec "TRUNCATE TABLE #{schema_name}.#{dst_table_name}" if table_exists?(conn, schema_name, dst_table_name)
        #conn.exec "INSERT INTO #{schema_name}.#{dst_table_name}\n(SELECT * FROM #{schema_name}.#{src_table_name})"
        #conn.exec "DROP TABLE #{schema_name}.#{src_table_name}"
      end

    end
  end

  def schema_names
    with_connection do |conn|
      sql = <<-SQL.strip_heredoc
      SELECT schema_name
      FROM information_schema.schemata
      WHERE schema_name <> 'information_schema'
      AND schema_name NOT LIKE 'pg_%'
      SQL

      rs = conn.exec sql
      rs.values.map(&:first)
    end
  end

  def copy_from_file(schema_name, table_name, csv_file, options={})
    with_connection do |conn|
      schema_name = conn.escape_string(schema_name)
      table_name = conn.escape_string(table_name)

      conn.copy_data "COPY #{schema_name}.#{table_name} FROM STDIN CSV #{options[:header]?'HEADER':''}" do
        buf = ''
        while csv_file.read(256, buf)
          conn.put_copy_data(buf)
        end
      end

    end
  end

  def column_definitions(schema_name, table_name)
    with_connection do |conn|
      sql = <<-SQL.strip_heredoc
        SELECT 
          c.relname, a.attname AS column_name,
          pg_catalog.format_type(a.atttypid, a.atttypmod) as type,
          case 
            when a.attnotnull
          then 'NOT NULL' 
          else 'NULL' 
          END as not_null 
        FROM pg_class c,
         pg_attribute a,
         pg_type t,
         pg_namespace n
         WHERE c.relname = '#{table_name}'
         AND n.nspname = '#{schema_name}'
         AND a.attnum > 0
         AND a.attrelid = c.oid
         AND a.atttypid = t.oid
         AND c.relnamespace = n.oid
       ORDER BY a.attnum
      SQL

      puts sql
      rs = conn.exec sql

      rs.values.map do |col|
        {name: col[1], type: col[2], null: col[3]}
      end
    end
  end

  def get_create_table_statement(schema_name, table_name)
    columns = column_definitions(schema_name, table_name)

    statement = "CREATE TABLE #{schema_name}.#{table_name} (\n"
    columns.each_with_index do |col, index|
      statement << "  #{col[:name]}  #{col[:type]}  #{col[:null]}"
      statement << ',' if index != columns.size - 1
      statement << "\n"
    end
    statement << ");\n"

    statement
  end

  private

  def with_connection(&block)
    conn = get_connection

    block.call(conn)
  ensure
    conn.close if not conn.nil?
  end

  def get_connection
    PG::Connection.connect(@dbconfig)
  end

  def column_line(column)
    name, data_type, nullable = column.symbolize_keys.values_at(:column_name, :data_type, :is_nullable)

    # default type to varchar
    data_type ||= "VARCHAR(1000)"

    line_tokens = ["\"#{name}\""]
    line_tokens << data_type
    line_tokens << (nullable ? '' : 'NOT NULL')

    line_tokens
      .select { |token| token != '' }
      .join " "
  end

  def create_table_statement(connection, columns, table_name, options={})
    statement = "CREATE #{options[:temporary] ? 'TEMPORARY' : ''} TABLE #{connection.escape_string(table_name)} (\n"
    statement << columns
      .map { |column| column_line(column) }
      .map(&:strip)
      .map { |column| connection.escape_string(column) }
      .join(",\n")
    statement << "\n);"

    statement
  end

  def table_exists?(connection, schema_name, table_name)
    sql = <<-SQL.strip_heredoc
        SELECT
          count(table_name)
        FROM
          information_schema.tables
        WHERE
          table_schema <> 'pg_catalog'
          AND table_schema <> 'information_schema'
          AND table_schema !~ '^pg_toast'
          AND table_schema = '#{connection.escape_string(schema_name)}'
          AND table_name = '#{connection.escape_string(table_name)}'
        GROUP BY
          table_schema,table_name;
    SQL

    res = connection.exec(sql)

    res.values.size > 0
  end
end
