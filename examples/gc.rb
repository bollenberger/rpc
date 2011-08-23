# Example of how to call the garbage collector.

require 'odb.rb'
puts ObjectDB.new('dbi:Pg:pgsql', 'pgsql').gc
