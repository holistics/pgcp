require 'logger'

class PgcpRunner < Thor
  desc 'cp', 'Perform copies of tables between Postgres databases'
  method_option :source, type: :string, aliases: '-s', desc: 'Source database', required: true
  method_option :dest, type: :string, aliases: '-d', desc: 'Destination database', required: true
  method_option :table, type: :string, aliases: '-t', desc: 'Table to be copied', required: true
  method_option :config, type: :string, aliases: '-c', desc: 'Path to config file'
  method_option :force_schema, type: :string, aliases: '-f', desc: 'Force destination schema'
  method_option :log, type: :string, aliases: '-l', desc: 'Path to log file'

  def cp
    config = load_config_file(options['config'] || File.join(ENV['HOME'], '.pgcp.yml'))
    if options['log']
      Pgcp.log_file = options['log']
    end

    src = config['databases'][options['source']].symbolize_keys!
    dest = config['databases'][options['dest']].symbolize_keys!

    begin
      tr = Transport.new(src, dest)
      if options['table'].include? '*'
        if (not options['table'].include? '.') or (options['table'].count('.') > 1)
          Pgcp.logger.error 'Globbed tables must have schema name, e.g. public.test* is valid but test* is not.'
          return
        end

        tr.copy_tables(options['table'], force_schema: options['force_schema'])
      else
        tr.copy_table(options['table'], nil, force_schema: options['force_schema'])
      end
    rescue Exception => e
      Pgcp.logger.error(e.message)
      return
    end
  end

  default_task :cp

  private
  def load_config_file(path)
    config = {}
    if not path.nil? and File.exists?(path)
      config = YAML::load_file(path)
    end

    config
  end
end


class Pgcp
  @@logger = nil

  def self.logger
    if not @@logger
      @@logger = Logger.new STDOUT
    end

    @@logger
  end

  def self.log_file=(path)
    @@logger = Logger.new(path)
  end
end
