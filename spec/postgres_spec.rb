require 'pgcp/transport'

RSpec.describe Postgres do
  it "should be able to get index names from a table" do
    src = {host: 'localhost', dbname: 'dashboard_dev', user: 'postgres'}

    p = Postgres.new(src)
    p.index_names('public', 'users')
  end

  it "should be able to get indexes from a table" do
    src = {host: 'localhost', dbname: 'dashboard_dev', user: 'postgres'}

    p = Postgres.new(src)
    p.get_indexes('public', 'users')
  end
end
