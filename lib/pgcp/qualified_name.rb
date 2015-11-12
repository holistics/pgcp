class QualifiedName
  attr_accessor :schema_name, :table_name

  def initialize(schema_name, table_name=nil)
    if table_name.nil?
      @table_name = schema_name.split('.')[1]
      @schema_name = schema_name.split('.')[0]
    else
      @schema_name = schema_name
      @table_name = table_name
    end
  end

  def full_name
    "#{@schema_name}.#{@table_name}"
  end

  def to_s
    full_name
  end
end
