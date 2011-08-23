# Create supporting syntax for the Transparent module
class Module
    def redirect(*names)
        names.each do |name|
            module_eval %Q{def #{name}(*args, &block)
                method_missing(#{name.inspect}, *args, &block)
                end}
        end
    end
end

# Mixin. Use to make the standard Object methods be passed through to
# method_missing so that they can be handled differently.
module Transparent
    Object.instance_methods(true).each do |method|
        unless method[0..1]=='__'
            redirect method
        end
    end
    
    alias_method :transparent_kind_of?, :kind_of?
    def kind_of?(*args)
        if args==[Transparent]
            true
        else
            transparent_kind_of?(*args)
        end
    end
end
