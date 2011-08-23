# Initialize the database with an employee record.

require 'odb.rb'
require 'employee.rb'

odb = ObjectDB.new('dbi:Pg:pgsql', 'pgsql')
odb.create(Employee.new('brian', 2))
