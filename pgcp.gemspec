Gem::Specification.new do |s|
  s.name        = 'pgcp'
  s.version     = '0.0.0'
  s.date        = '2015-11-05'
  s.summary     = "A simple command line tool to copy tables from one Postgres database to another"
  s.description = "A simple command line tool to copy tables from one Postgres database to another"
  s.authors     = ["Thanh Dinh Khac", "Huy Nguyen"]
  s.email       = 'thanh@holistics.io'
  s.executables << 'pgcp'
  s.files       = %w(lib/pgcp.rb lib/postgres.rb lib/transport.rb)
  s.homepage    =
    'http://rubygems.org/gems/pgcp'
  s.license       = 'GPL'
  s.add_runtime_dependency "activesupport", "~> 4.2"
  s.add_runtime_dependency "thor", "~> 0.19"
  s.add_runtime_dependency "pg", "~> 0.18"
end
