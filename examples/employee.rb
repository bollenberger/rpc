# Simple employee object for demonstration.

class Employee
    attr_accessor :name, :salary

    def initialize(name, salary)
        @name, @salary = name, salary
    end

    def raise!(percent)
        @salary += @salary*(percent/100.0)
    end
end
