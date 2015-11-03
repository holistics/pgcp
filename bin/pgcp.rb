#!/usr/bin/env ruby
require 'thor'

class PgcpRunner < Thor
  desc 'cp', 'Perform copies of tables'
  def cp

  end

  default_task :cp
end

PgcpRunner.start
