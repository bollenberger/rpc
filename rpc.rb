# Ruby RPC Copyright 2005 Brian Ollenberger

require 'socket'
require 'thread'
require 'odb/transparent'

# The garbage collection state data structure.
class GCSet
    def initialize
        @count = {}
        @refs = {}
    end
    
    def add(id)
        if @count.has_key?(id)
            @count[id] = @count[id] + 1
        else
            @count[id] = 1
            @refs[id] = ObjectSpace._id2ref(id)
        end
    end
    
    def delete(id)
        @count[id] = @count[id] - 1
        if @count[id]<1
            @count.delete(id)
            @refs.delete(id)
        end
    end
    
    def contains?(id)
        @refs.has_key?(id)
    end
end

# Pass a parameter or return value explicitly by value (by copy).
class ByVal
    def initialize(object)
        @object = object
    end
    
    attr_accessor :object
end

# Pass a parameter or return value explicity by reference. This is only useful
# for the RemoteReference::CopyTypes.
class ByRef
    def initialize(object)
        @object = object
    end
    
    attr_accessor :object
end

# Instead of giving remote references to remote references back to the
# owner of an object, we give it a LocalObject. This peephole optimization
# actually extends rather nicely to transitive remote references to
# remote references.
class LocalReference
    def initialize(id)
        @id = id
    end
    
    def method_missing(name, *args, &block)
        raise "Attempt to use an inactive LocalReference" if @o.nil?
        
        @o.send(name, *args, &block)
    end
    
    def _activate
        @o = ObjectSpace._id2ref(@id)
        extend(Transparent)
    end
end

# A transparent reference to a remote object.
class RemoteReference
    alias_method :_nil?, :nil?
    include Transparent
    alias_method :_kind_of?, :kind_of?
    alias_method :object_id, :__id__
    def kind_of?(*args)
        if args==[RemoteReference]
            return true
        end
        _kind_of?(*args)
    end
    
    CopyTypes = [Bignum, Class, FalseClass, Fixnum, Float, Integer, NilClass,
                Numeric, Range, String, Symbol, TrueClass]
    
    def RemoteReference.new(object, connection)
        begin
            if object.kind_of?(RemoteReference)
                if object._connection.nil?
                    return object
                elsif object._connection.__id__ == connection.__id__
                    # What if a ByRef is used?
                    return LocalReference.new(object._get_id)
                end
            end
        rescue
            raise # Remind me why I was suppressing exceptions here and I'll
                  # remove raise and replace it with a comment explaining it.
        end
        
        if object.kind_of?(ByVal)
            object.object
        elsif object.kind_of?(ByRef)
            super(object.object, connection)
        elsif not object.kind_of?(Transparent) and
        CopyTypes.include?(object.class) or
        object.kind_of?(Exception)
            object
        else
            super(object, connection)
        end
    end
    
    def initialize(object, connection)
        @id = object.object_id
        connection._gc_add(@id)
    end
    
    def _get_id
        @id
    end
    
    def method_missing(name, *args, &block)
        if @_connection.nil?
            ObjectSpace._id2ref(@id).send(name, *args, &block)
        else
            @_connection._call_method(@id, name, *args, &block)
        end
    end
    
    attr_accessor :_connection
end

# A method call message.
class MethodCall
    def initialize(connection, threadid, id, block, name, *args)
        @threadid, @id, @name = threadid, id, name
        @args = []
        args.each do |arg|
            @args << RemoteReference.new(arg, connection)
        end
        @block = RemoteReference.new(block, connection)
    end
    
    attr_reader :threadid, :id, :name, :args, :block
end

# A return value or exception return message.
class ReturnValue
    def initialize(exception, object, threadid, connection)
        @exception, @threadid = exception, threadid
        #print "returning: ", object._connection.nil?, "\n"
        @object = RemoteReference.new(object, connection)
    end
    
    attr_reader :exception, :threadid, :object
end

# A garbage found message. Used for distributed GC.
class GarbageFound
    def initialize(id)
        @object_id = id
    end
    
    attr_reader :object_id
end

# A connection to a remote machine. This is typically used to connect to a
# remote Server, though it is also used in the Server to represent its clients.
class Connection
    alias_method :_nil?, :nil?
    include Transparent
    alias_method :nil?, :_nil?
    
    def initialize(server='localhost', port=Server::DefaultPort)
        @send_mutex = Mutex.new
        @dead = false
        @gc = GCSet.new
        
        @object = nil
        
        if server.kind_of?(String)
            server = TCPSocket.new(server, port)
        elsif server.kind_of?(Array)
            @object = server[1]
            server = server[0]
        end
        
        @connection = server
        @responses = {}
        @waiting_threads = {}
        
        # Wait and watch for incoming messages
        @thread = Thread.new do
            begin
                while true
                    incoming = _receive
                    
                    if incoming.kind_of?(MethodCall)
                        Thread.new(incoming) do |call|
                            begin
                                if call.id.nil?
                                    object = @object
                                else
                                    # Do a security check.
                                    unless @gc.contains?(call.id)
                                        # This will only happen to somebody if
                                        # they try to remotely access an object
                                        # by ID to which they were never given
                                        # a reference.
                                        raise 'No reference given for that '+
                                            'remote object'
                                    end
                                    object = ObjectSpace._id2ref(call.id)
                                end

                                result = object.send(call.name,
                                *call.args) do |*args|
                                    # Call the block back
                                    call.block.call(*args)
                                end
                                
                                _send(ReturnValue.new(false, result,
                                    call.threadid, self))
                            rescue => exception
                                _send(ReturnValue.new(true, exception,
                                    call.threadid, self))
                            end
                        end
                    elsif incoming.kind_of?(ReturnValue)
                        unless @waiting_threads.delete(
                            incoming.threadid).nil?
                            @responses[incoming.threadid] = incoming
                            ObjectSpace._id2ref(incoming.threadid).wakeup
                        end
                    elsif incoming.kind_of?(GarbageFound)
                        @gc.delete(incoming.object_id)
                    end
                end
            rescue EOFError, Errno::ECONNRESET
                # Ignore this error, as it is just a client disconnecting.
                # Remote references will already act invalid by virtue of the
                # remote host disconnecting, so we don't need
                # to track it here.
            end
        end
    end
    
    def join
        @thread.join
    end
    
    def close
        @connection.close
    end
    
    # Make a finalizer which will send a GarbageFound message and run in its
    # own thread. The separate thread is to avoid a potential deadlock if
    # a finalizer runs in a thread that already holds the send mutex.
    # An alternative would be to make the send mutex reentrant.
    # See also the comment in Connection::_send
    def _make_finalizer(connection, remote_id)
        lambda do |id|
            Thread.new do
                begin
                    connection._send(GarbageFound.new(remote_id))
                rescue => e
                    # We may be "Unable to reach peer" here. That's OK.
                    # Since the GarbageFound message is really advisory only.
                end
            end
        end
    end
    
    def _set_finalizer(o)
        ObjectSpace.define_finalizer(o,
            _make_finalizer(o._connection, o._get_id))
    end
    
    def _gc_add(id)
        @gc.add(id)
    end
    
    def _receive
        select([@connection],nil,nil)
        o=nil
        begin
            o=Marshal.load(@connection, lambda { |o|
                begin
                    if o.kind_of?(RemoteReference)
                        o._connection = self
                        _set_finalizer(o)
                    elsif o.kind_of?(LocalReference)
                        o._activate
                    end
                rescue
                    raise # This was eating exceptions. Explain why and I'll
                          # put a comment here explaining it. At this point
                          # I'm thinking that catching any exceptions here is
                          # unnecessary.
                end
            })
        rescue => e
            @dead = true
            raise e
        end
        o
    end
    
    def _send(o)
        if @dead
            raise "Unable to reach peer"
        end
        
        # We must dump outside of the mutex since it can cause _send to be
        # called reentrantly. Another solution is to still do
        # Marshal.dump(o, @connection) in the mutex (or not) but make
        # @send_mutex reentrant.
        # See also the comment in Connection::_make_finalizer
        data = Marshal.dump(o)
        
        @send_mutex.synchronize do
            @connection.write(data)
        end
    end
    
    def method_missing(name, *args, &block)
        # Pass method calls to the root object of the server
        _call_method(nil, name, *args, &block)
    end
  
    def _call_method(id, name, *args)
        mc = MethodCall.new(self, Thread.current.object_id, id,
            lambda { |*x| yield(*x) }, name, *args)
        @waiting_threads[Thread.current.object_id] = self
        _send(mc)
        Thread.stop
        
        response = @responses.delete(Thread.current.object_id)
        if response.exception
            # Tweak backtrace
            begin
                raise 'dummy'
            rescue => e
                # Tack on the local backtrace and remove rpc library frames.
                me = e.backtrace[0].split(':')[0]
                backtrace = response.object.backtrace+e.backtrace[0..-1]
                backtrace.delete_if do |frame|
                    frame.split(':')[0]==me
                end
                
                response.object.set_backtrace(backtrace)
                
                raise response.object
            end
        else
            return response.object
        end
    end
end

# A server that exports a single object on a socket.
class Server
    DefaultPort = 4000

    def initialize(object, server=DefaultPort, listen_on='0.0.0.0')
        if server.kind_of?(Integer)
            server = TCPServer.new(listen_on, server)
        end
        
        @thread = Thread.new do
            while true
                Connection.new([server.accept, object])
            end
        end
    end
    
    def join
        @thread.join
    end
end
