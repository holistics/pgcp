require 'active_support'
require 'active_support/core_ext'
require './lib/postgres'
require './lib/qualified_name'
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

  def copy_table(src_tablename, dest_tablename=nil)
    dest_tablename ||= src_tablename
    src_table = QualifiedName.new(src_tablename)
    dest_table = QualifiedName.new(dest_tablename)

    src_conn = Postgres.new(@src_dbconfig)
    dest_conn = Postgres.new(@dest_dbconfig)

    if dest_conn.table_exist?(src_table.schema_name, src_table.table_name)
      temp_table = QualifiedName.new("#{dest_table.schema_name}.temp_#{SecureRandom.hex}")
      create_table_statement =
        src_conn.get_create_table_statement(src_table.schema_name,
                                            src_table.table_name,
                                            temp_table.schema_name,
                                            temp_table.table_name)
      dest_conn.exec(create_table_statement)
      direct_copy(src_table.full_name, temp_table.full_name)
      dest_conn.hotswap_table(dest_table.schema_name, temp_table.table_name, dest_table.table_name)
    else
      create_table_statement =
        src_conn.get_create_table_statement(src_table.schema_name,
                                            src_table.table_name,
                                            dest_table.schema_name,
                                            dest_table.table_name)
      dest_conn.exec(create_table_statement)
      direct_copy(src_table.full_name, dest_table.full_name)
    end
  end

  private

  def direct_copy(src_tablename, dest_tablename)
    sql_in = sql_copy_from_stdin(dest_tablename)
    sql_out = sql_copy_to_stdout(src_tablename)
    command = transfer_command(@src_dbconfig, @dest_dbconfig, sql_in, sql_out)
    `#{command}`
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
