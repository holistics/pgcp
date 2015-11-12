require 'logger'

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
