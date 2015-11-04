require 'active_support'
require 'active_support/core_ext'

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

    sql_in = sql_copy_from_stdin(dest_tablename)
    sql_out = sql_copy_to_stdout(src_tablename)
    command = transfer_command(@src_dbconfig, @dest_dbconfig, sql_in, sql_out)
    `#{command}`
  end

  private

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
