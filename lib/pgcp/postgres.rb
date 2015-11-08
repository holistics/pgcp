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

  def list_tables(schema_name)
    with_connection do |conn|
      sql = <<-SQL.strip_heredoc
        SELECT table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_schema NOT IN ('pg_catalog', 'information_schema')
          AND table_schema = '#{schema_name}'
        ORDER BY 1
      SQL

      rs = conn.exec sql

      rs.values.map(&:first)
    end

  end

  def drop_table(schema_name, table_name)
    with_connection do |conn|
      if _table_exists?(conn, schema_name, table_name)
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
        unless _table_exists?(conn, schema_name, table_name)
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
        conn.exec "DROP TABLE #{schema_name}.#{dst_table_name}" if _table_exists?(conn, schema_name, dst_table_name)
        conn.exec "ALTER TABLE #{schema_name}.#{src_table_name} RENAME TO #{dst_table_name}"
        #conn.exec "TRUNCATE TABLE #{schema_name}.#{dst_table_name}" if _table_exists?(conn, schema_name, dst_table_name)
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

      rs = conn.exec sql

      rs.values.map do |col|
        {name: col[1], type: col[2], null: col[3]}
      end
    end
  end

  def index_names(schema_name, table_name)
    with_connection do |conn|
      sql = <<-SQL.strip_heredoc
        SELECT
            C.relname AS "index_name"
        FROM pg_catalog.pg_class C,
             pg_catalog.pg_namespace N,
             pg_catalog.pg_index I,
             pg_catalog.pg_class C2
        WHERE C.relkind IN ( 'i', '' )
          AND N.oid = C.relnamespace
          AND N.nspname = '#{schema_name}'
          AND I.indexrelid = C.oid
          AND C2.oid = I.indrelid
          AND C2.relname = '#{table_name}';
      SQL

      rs = conn.exec sql

      rs.values.map(&:first)
    end
  end

  def get_indexes(schema_name, table_name)
    idx_names = self.index_names(schema_name, table_name)

    with_connection do |conn|
      idx_names.map do |name|
        index_info = index_info(conn, name, schema_name)
        index_info['name'] = name
        index_info['columns'] = index_column_names conn, index_info['oid']

        index_info
      end
    end
  end

  def index_info(conn, index_name, schema_name)
    sql = <<-SQL.strip_heredoc
    SELECT
        C.oid,
        I.indisunique AS "unique",
        I.indisprimary AS "primary",
        pg_get_expr(I.indpred, I.indrelid) AS "where"
    FROM pg_catalog.pg_class C,
         pg_catalog.pg_namespace N,
         pg_catalog.pg_index I
    WHERE C.relname = '#{index_name}'
      AND C.relnamespace = N.oid
      AND I.indexrelid = C.oid
      AND N.nspname = '#{schema_name}';
    SQL

    rs = conn.exec sql
    rs[0].tap do |info|
      info['unique'] = info['unique'] != 'f'
      info['primary'] = info['primary'] != 'f'
      info['where'] = info['where'][1..-2] if info['where'].present?
    end
  end

  def index_column_names conn, oid
    sql = <<-SQL.strip_heredoc
    SELECT
         pg_catalog.pg_get_indexdef(A.attrelid, A.attnum, TRUE) AS "column_name"
    FROM pg_catalog.pg_attribute A
    WHERE A.attrelid = $1
      AND A.attnum > 0
      AND NOT A.attisdropped
    ORDER BY A.attnum;
    SQL
    conn.exec(sql, [oid]).map { |row| row['column_name'] }
  end

  def create_indexes(schema_name, table_name, indexes)
    with_connection do |conn|
      indexes.each do |index|
        if index['primary']
          sql = <<-SQL.strip_heredoc
            ALTER TABLE #{schema_name}.#{table_name} ADD PRIMARY KEY (#{index['columns'][0]})
          SQL
        else
          sql = <<-SQL.strip_heredoc
          CREATE #{index['unique'] ? 'UNIQUE': ''} INDEX #{index['name']}
          ON #{schema_name}.#{table_name} (#{index['columns'].join(', ')})
          #{index['where'] ? 'WHERE ' + index['where'] : ''}
          SQL
        end

        conn.exec(sql)
      end
    end
  end

  def get_create_table_statement(src_schema_name, src_table_name, dest_schema_name=nil, dest_table_name=nil)
    dest_schema_name ||= src_schema_name
    dest_table_name ||= dest_schema_name

    columns = column_definitions(src_schema_name, src_table_name)

    statement = "CREATE TABLE #{dest_schema_name}.#{dest_table_name} (\n"
    columns.each_with_index do |col, index|
      statement << "  #{col[:name]}  #{col[:type]}  #{col[:null]}"
      statement << ',' if index != columns.size - 1
      statement << "\n"
    end
    statement << ");\n"

    statement
  end

  def table_exist?(schema_name, table_name)
    with_connection do |conn|
      _table_exists?(conn, schema_name, table_name)
    end
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

  def _table_exists?(connection, schema_name, table_name)
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
