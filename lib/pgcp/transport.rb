require 'active_support'
require 'active_support/core_ext'
require 'pgcp/postgres'
require 'pgcp/qualified_name'
require 'securerandom'

class Transport
  # Initialize Transport instance
  #
  # @param [Hash] src_dbconfig Source database config
  # @param [Hash] dest_dbconfig Destination database config
  def initialize(src_dbconfig, dest_dbconfig, options={})
    @src_dbconfig = src_dbconfig
    @src_dbconfig[:port] ||= 5432
    @dest_dbconfig = dest_dbconfig
    @dest_dbconfig[:port] ||= 5432
  end

  def copy_tables(src_tablenames, options={})
    schema_name = src_tablenames.split('.')[0]
    table_glob = src_tablenames.split('.')[1]

    dest_conn = Postgres.new(@dest_dbconfig)
    tables = dest_conn.list_tables(schema_name)
    tables.each do |table|
      if File.fnmatch(table_glob, table)
        copy_table("#{schema_name}.#{table}", options)
      end
    end
  end

  def copy_table(src_tablename, dest_tablename=nil, options={})
    dest_tablename ||= src_tablename
    options[:create_schema] = true if options[:create_schema].nil?
    options[:skip_indexes] = false if options[:skipe_indexes].nil?

    Pgcp.logger.info "Start to copy from table #{src_tablename} to table #{dest_tablename}"
    src_table = QualifiedName.new(src_tablename)
    dest_table = QualifiedName.new(dest_tablename)
    dest_table.schema_name = options[:force_schema] if not options[:force_schema].nil?

    src_conn = Postgres.new(@src_dbconfig)
    dest_conn = Postgres.new(@dest_dbconfig)

    dest_conn.exec "CREATE SCHEMA IF NOT EXISTS #{dest_table.schema_name};" if options[:create_schema]

    src_indexes = src_conn.get_indexes(src_table.schema_name, src_table.table_name)
    if dest_conn.table_exist?(src_table.schema_name, src_table.table_name)
      Pgcp.logger.info "Destination table already exists, creating temporary table"
      temp_table = QualifiedName.new("#{dest_table.schema_name}.temp_#{SecureRandom.hex}")
      create_table_statement =
        src_conn.get_create_table_statement(src_table.schema_name,
                                            src_table.table_name,
                                            temp_table.schema_name,
                                            temp_table.table_name)
        begin
          dest_conn.exec(create_table_statement)
          Pgcp.logger.info "Copying table data to temporary table. This could take a while..."
          direct_copy(src_table.full_name, temp_table.full_name)
          Pgcp.logger.info "Hotswapping to destination table #{dest_tablename}"
          dest_conn.hotswap_table(dest_table.schema_name, temp_table.table_name, dest_table.table_name)
          Pgcp.logger.info "Done copying table data."
        rescue Exception => e
          Pgcp.logger.error(e.message)
          return
        ensure
          dest_conn.drop_table(temp_table.schema_name, temp_table.table_name)
        end
    else
      Pgcp.logger.info "Destination table does not exist, creating destination table."
      create_table_statement =
        src_conn.get_create_table_statement(src_table.schema_name,
                                            src_table.table_name,
                                            dest_table.schema_name,
                                            dest_table.table_name)
        dest_conn.exec(create_table_statement)
        Pgcp.logger.info "Copying table data to destination table. This could take a while..."
        direct_copy(src_table.full_name, dest_table.full_name)
        Pgcp.logger.info "Copying table data to destination table done."
    end

    unless options[:skip_indexes]
      Pgcp.logger.info "Copying table indexes to destination table..."
      dest_conn.create_indexes(dest_table.schema_name, dest_table.table_name, src_indexes)
      Pgcp.logger.info "Done copying table indexes."
    end

  end

  private

  def direct_copy(src_tablename, dest_tablename)
    sql_in = sql_copy_from_stdin(dest_tablename)
    sql_out = sql_copy_to_stdout(src_tablename)
    command = transfer_command(@src_dbconfig, @dest_dbconfig, sql_in, sql_out)
    `#{command}`
    exit_code = `echo $?`.to_i
    if exit_code != 0
      raise 'Failed to directly copy data'
    end
  end

  def sql_copy_from_stdin(q_tablename)
    <<-SQL.strip_heredoc
      COPY #{q_tablename} FROM STDIN
    SQL
  end

  def sql_copy_to_stdout(q_tablename)
    <<-SQL.strip_heredoc
      COPY (SELECT * FROM #{q_tablename}) TO STDOUT
    SQL
  end

  def transfer_command(src_dbconfig, dest_dbconfig, sql_in, sql_out)
    copy_to_command = %Q{
      env PGPASSWORD="#{src_dbconfig[:password]}"
      psql
        -U #{src_dbconfig[:user]}
        -h #{src_dbconfig[:host]}
        -p #{src_dbconfig[:port]}
        -c "#{sql_out}"
    #{src_dbconfig[:dbname]}
    }.gsub(/\n/, ' ')
    copy_from_command = %Q{
      env PGPASSWORD="#{dest_dbconfig[:password]}"
      psql
        -U #{dest_dbconfig[:user]}
        -h #{dest_dbconfig[:host]}
        -p #{dest_dbconfig[:port]}
        -c "#{sql_in}"
    #{dest_dbconfig[:dbname]}
    }.gsub(/\n/, ' ')

    "#{copy_to_command} | #{copy_from_command}"
  end
end
