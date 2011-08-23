# Simple server that serves a single persistent "employee" object.

require 'odb'
require 'rpc'
require 'employee'

Thread.abort_on_exception = true

Server.new(ObjectDB.new('dbi:Pg:pgsql', 'pgsql')).join