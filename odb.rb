require 'dbi'
require 'thread'
require 'odb/transparent'

class PersistentArray
    def PersistentArray.allocate
        PersistentArray.new
    end
    
    def initialize(array=[])
        @data = {}
        array.each_index do |index|
            @data[index] = array[index]
        end
        @odb = nil
    end
    
    def set(odb, id)
        if @odb.nil?
            @id, @odb = id, odb
            @length = nil
        end
    end
    
    def length
        return @data.length if @odb.nil?
        return @length unless @length.nil?
        @odb.execute('select count(*) from arrays where id=?', @id) do |h|
            h.each do |row|
                return @length = row[0]
            end
        end
    end
    
    def [](index)
        index = index.to_i
        return @data[index] if @data.has_key?(index)
        return nil if @odb.nil?
        
        @data[index] = nil
        @odb.execute('select object from arrays where id=? and '+
            'index=?', @id, index) do |handle|
           
            handle.each do |row|
                @data[index] = PersistentObject.new(@odb, row['object'])
            end
        end

        @data[index]
    end

    def []=(index, value)
        index = index.to_i
        @data[index] = value
        if not @length.nil? and @length<index
            @length = index+1
        end
    end
    
    def <<(value)
        self[length] = value
    end
    
    def each_index
        0.upto(length-1) do |index|
            yield(index)
        end
    end
    
    def each
        each_index do |index|
            yield(self[index])
        end
    end
    
    def write
        #@odb.execute('delete from arrays where id=?', @id)
        @data.each do |key, value|
            id = @odb.writeObject(value)
            if 0==@odb.do('update arrays set object=? where id=? and index=?',
                id, @id, key)
                
                @odb.do('insert into arrays (id, index, object) values '+
                    '(?, ?, ?)', @id, key, id)
            end
        end
    end
end

class ObjectDB
    alias_method :_send, :send
    include Transparent
    alias_method :send, :_send
    alias_method :__send__, :_send
    def nil?
        false
    end
    
    # Static root object ID
    RootID = 0
    # Member names for special properties (must not start with @)
    MemberClass = 'c'
    MemberBase = 'b'
    # Types that must be stored in the database by serialized copy
    CopyTypes = [Bignum, Class, FalseClass, Fixnum, Float, Integer,
        NilClass, Numeric, Range, String, Symbol, TrueClass, Hash,
        Array]

    # Create the ObjectDB database with the given root object
    def create(o)
        raise "Root object must not be nil" if o.nil?
        
        synchronize do
            begin
                @dbh.execute('drop table arrays')
            rescue
            end
        end
        synchronize do
            begin
                @dbh.execute('drop table members')
            rescue
            end
        end
        synchronize do
            begin
                @dbh.execute('drop table objects')
            rescue
            end
        end
        
        synchronize do
            @dbh.execute('set client_min_messages = warning')
            
            @dbh.execute('create table objects ('+
                'id bigint,'+
                'class_name text default null,'+
                'value bytea default null,'+
                'gc boolean default false not null, '+
                'primary key (id),'+
                'unique (class_name))')
            
            @dbh.execute('create table members ('+
                'id bigint references objects '+
                    'on delete cascade,'+
                'member_name text,'+
                'member_id bigint references objects '+
                    'on delete restrict,'+
                'primary key (id, member_name))')
            
            @dbh.execute('create table arrays ('+
                'id bigint references objects '+
                    'on delete cascade,'+
                'index bigint,'+
                'object bigint references objects '+
                    'on delete restrict,'+
                'primary key (id, index))')

            writeObject(o)
        end
    end
    
    # Delegate database tasks to the database handle
    def do(*args)
        @dbh.do(*args)
    end
    def execute(*args)
        @dbh.execute(*args) do |handle|
            yield(handle)
        end
    end

    # Not all clients should probably garbage collect.
    # Perhaps one periodic garbage collection process for
    # the entire database.
    def gc
        count = 1
        synchronize do
            puts "Locking objects table exclusively..."
            @dbh.do('lock table objects')
            puts "Marking root..."
            @dbh.do('update objects set gc=true where id=?',
                RootID)
            print "Marking objects following links."
            while count>0
                count = @dbh.do(
                    'update objects set gc=true where id '+
                    'in (select o2.id '+
                    'from objects as o1, members, '+
                    'objects as o2 where o1.id=members.id '+
                    'and members.member_id=o2.id and o1.gc=true '+
                    'and o2.gc=false)')
                print "."
                count += @dbh.do(
                    'update objects set gc=true where id '+
                    'in (select o2.id '+
                    'from objects as o1, arrays, '+
                    'objects as o2 where o1.id=arrays.id '+
                    'and arrays.object=o2.id and o1.gc=true '+
                    'and o2.gc=false)')
                print "."
            end
            print "\n"
            puts "Deleting unmarked objects..."
            count = @dbh.do('delete from objects where gc=false')
            @dbh.do('update objects set gc=false')
        end
        puts "Vacuuming..."
        @dbh.do('vacuum')
        count
    end
    
    def ObjectDB.new(*args)
        super(DBI.connect(*args))
    end
    
    # Create ObjectDB given a database handle
    def initialize(dbh)
        @dbh = dbh
        
        @mutex = Mutex.new
        
        # Per transaction data structures
        @objects = {} # Objects loaded
        @saved = {} # New objects saved
    end
    
    # Get an object that is already loaded
    def existingObjects
        @objects
    end
    
    def readObject(id)
        return nil if id.nil?
        
        return @objects[id] if @objects.has_key?(id)
        
        # Load the object
        object = nil
        @dbh.execute('select value from objects where id=?',
            id) do |handle|
            
            object = handle.fetch['value']
            object = Marshal.load(object) unless object.nil?
            @objects[id] = object
        end
        
        # Load the object's members
        members = {}
        @dbh.execute('select member_name, member_id from members where id=?',
            id) do |handle|
            
            handle.each do |row|
                members[row['member_name']] = row['member_id']
            end
        end
        
        # Load the class of the object
        class_object = readObject(members.delete(MemberClass))
        
        # Allocate an empty object if we must
        if object.nil?
            class_object = Object if class_object.nil?
            object = class_object.allocate
            if object.class==PersistentArray
                object.set(self, id)
            end
            @objects[id] = object
        end
        
        # Load the base class if there is one
        base_class = readObject(members.delete(MemberBase))
        
        # Load in references to the member objects of this object
        # Note: All of this eval-ing is a potential security risk
        members.each do |name, member_id|
            odb = self
            value = 'nil'
            unless member_id.nil?
                value = "PersistentObject.new("+
                    "odb, #{member_id})"
            end
            if name[0,2]=='@@' # Class variable
                object.module_eval("#{name}=#{value}")
            else # Instance variable
                object.instance_eval("#{name}=#{value}")
            end
        end
        
        if object.class==PersistentArray
            object.set(self, id)
        end
        
        # Store the object in memory
        object
    end
    
    def writeObject(o)
        if o.nil?
            return nil
        end
        
        if @saved.has_key?(o.object_id)
            return @saved[o.object_id]
        end
        
        isPersistentObject = true
        id = nil
        object = o
        begin
            id = o._persistent_object_id
            object = o._persistent_object
        rescue
            isPersistentObject = false
        end

        if isPersistentObject # Already in the database
            return id if object.nil? # Don't write invalid objects
            
            value = nil
            if CopyTypes.include?(o.class)
                value = Marshal.dump(object)
            end
            @dbh.execute('update objects set value=? '+
                'where id=?', _escape_binary(value), id)
            
            @dbh.execute('delete from members where id=?', id)
        else
            exists_in_db = false
            class_name = nil            
            if o.class==Class
                class_name = o.to_s
                
                @dbh.execute(
                    'select id from objects where class_name=?',
                    class_name) do |handle|
                    
                    handle.each do |row|
                        # If the class is already in the database
                        id = row['id']
                        exists_in_db = true
                    end
                end
            end
            
            unless exists_in_db
                value = nil
                if CopyTypes.include?(o.class)
                    value = Marshal.dump(o)
                end
                
                id = newID
                @dbh.execute('insert into objects '+
                    '(id, class_name, value) '+
                    'values (?, ?, ?)', id, class_name, _escape_binary(value))
            else
                @dbh.execute('delete from members where id=?', id)
            end
        end
        
        @saved[o.object_id] = id
        
        # Write children or write the array contents if it a persistent array
        if object.class==PersistentArray
            object.set(self, id)
            object.write
        else
            object.instance_variables.each do |name|
                value = object.instance_eval(name)
            
                child_id = writeObject(value)
            
                @dbh.execute('insert into members '+
                    'values (?, ?, ?)', id, name,
                    child_id)
            end
        end
        
        # Write class
        @dbh.execute('insert into members values (?, ?, ?)',
            id, MemberClass, writeObject(object.class))
        
        # If the object is a class
        # Write base class and class variables
        if object.class==Class or object.class==Module
            object.class_variables.each do |name|
                value = object.module_eval(name)
                
                # Similar to instance variables above
                child_id = (begin
                    value._persistent_object_id
                rescue
                    writeObject(value)
                end)
                    
                @dbh.execute('insert into members '+
                    'values (?, ?, ?)', id, name,
                    child_id)
            end
        end
        
        if object.class==Class
            @dbh.execute('insert into members '+
                'values (?, ?, ?)', id, MemberBase,
                writeObject(object.superclass))
        end
        
        # Return the ID of the object
        id
    end
    
    # Get the next object ID
    def newID
        new_id = (@dbh.execute('select max(id)+1 '+
            'from objects').fetch)[0]
        if new_id.nil?
            RootID
        else
            new_id
        end
    end
    
    # Perform a synchronous transaction, passing the root object
    def transaction(readonly = false)
        synchronize do
            root = PersistentObject.new(self, RootID)
            
            # Call the client's code back
            begin
                result = yield(root)
            rescue => e
                @objects.clear
                @saved.clear
                raise e
            end
            
            writeObject(root) unless readonly
            
            # Forget about all loaded objects and classes
            @objects.clear
            @saved.clear
            
            result
        end
    end
    
    # Pass calls to the ObjectDB to the root object
    def method_missing(name, *args)
        result = nil
        transaction do |root|
            result = root.send(name, *args) do |*args|
                yield(*args)
            end
        end
        result
    end
    
private

    # Perform an operation in a transaction
    def synchronize
        @mutex.synchronize do
            begin
                @dbh.execute('begin')
                @dbh.execute('set transaction isolation level serializable')
                yield
            rescue DBI::ProgrammingError => e
                # Retry if the transaction fails due to some constraint
                # violation or inability to serialize the transaction.
                @dbh.execute('abort')
                sleep rand
                retry
            rescue => e
                @dbh.execute('abort')
                raise e
            end
            @dbh.execute('commit')
        end
    end
    
    def _escape_binary(str)
        if str.nil?
            nil
        else
            # PGconn.escape_bytea(value) - broken, it seems
            a = str.split(/\\/, -1).collect! do |s|
                s.gsub!(/'/,    "\\\\047")  # '  => \\047
                s.gsub!(/\000/, "\\\\000")  # \0 => \\000
                s
            end
            a.join("\\\\")
        end
    end
end

class PersistentObject
    alias_method :_kind_of?, :kind_of?
    include Transparent
    
    def PersistentObject.new(odb, id)
        existing_object = odb.existingObjects[id]
        if existing_object.nil?
            super(odb, id)
        else
            existing_object
        end
    end
    
    def initialize(odb, id)
        @odb = odb
        @id = id
        @object = nil
    end
    
    def _persistent_object_id
        @id
    end
    
    def _persistent_object
        @object
    end
    
    def send(name, *args)
        method_missing(name, *args) do |*args|
            yield(*args)
        end
    end
    
    def method_missing(name, *args)
        # If the object is invalid, bring it in from the database
        if @object.nil? and not @id.nil?
            @object = @odb.readObject(@id)
        end
        
        @object.send(name, *args) do |*args|
            yield(*args)
        end
    end
end
