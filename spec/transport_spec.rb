require 'transport'

RSpec.describe Transport do
  context "Directly copy table from one database to another" do
    let (:src) {
      {host: 'localhost', dbname: 'dashboard_dev', user: 'postgres'}
    }
    let (:dest) {
      {host: 'localhost', dbname: 'dashboard_test', user: 'postgres'}
    }

    it "should be able to create the Transport class" do
      tr = Transport.new(src, dest)
      tr.copy_table('public.users')
    end

    it "should be able to directly copy table" do
      tr = Transport.new(src, dest)
      tr.copy_table('public.users')
    end

    it "should not create schema if specified" do
      tr = Transport.new(src, dest)
      tr.copy_table('public.users', create_schema: false)
    end

    it "should skip copy indexes if specified" do
      tr = Transport.new(src, dest)
      tr.copy_table('public.users', skip_indexes: true)
    end
  end
end
