require 'qualified_name'

RSpec.describe QualifiedName do
  it "should be able to be created with both arguments" do
    q = QualifiedName.new('public', 'test')

    expect(q.schema_name).to eq('public')
    expect(q.table_name).to eq('test')
  end

  it "should be able to be created with single argument" do
    q = QualifiedName.new('public.test')

    expect(q.schema_name).to eq('public')
    expect(q.table_name).to eq('test')
  end
end
