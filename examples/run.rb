# Simple client that connects to the employee object server,
# gives the employee a 10% raise, then prints out the employee's information.

require 'rpc'

employee = Connection.new('localhost')

employee.raise!(10)
puts employee.inspect