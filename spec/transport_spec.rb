require 'transport'

RSpec.describe Transport do
  context "Directly copy table from one database to another" do
    it "should be able to create the Transport class" do
      src = {host: 'localhost', dbname: 'dashboard_dev', user: 'postgres'}
      dest = {host: 'localhost', dbname: 'dashboard_test', user: 'postgres'}

      tr = Transport.new(src, dest)
      tr.copy_table('public.users')
    end
  end
end
