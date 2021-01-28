import hazelcast
from hazelcast.errors import HazelcastSerializationError

from hazelcast.serialization.api import Portable

# This sample code demonstrates multiversion support of Portable serialization.

# With multiversion support, you can have two members that have different
# versions of the same object, and Hazelcast will store both meta information and use the
# correct one to serialize and deserialize portable objects depending on the member.


# Default (version 1) Employee class.
class Employee(Portable):
    FACTORY_ID = 666
    CLASS_ID = 1

    def __init__(self, name=None, age=None):
        self.name = name
        self.age = age

    def write_portable(self, writer):
        writer.write_string("name", self.name)
        writer.write_int("age", self.age)

    def read_portable(self, reader):
        self.name = reader.read_string("name")
        self.age = reader.read_int("age")

    def get_factory_id(self):
        return self.FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID

    def __str__(self):
        return "Employee(name:%s, age:%s)" % (self.name, self.age)

    def __eq__(self, other):
        return type(self) == type(other) and self.name == other.name and self.age == other.age


# If you update the class by changing the type of one of the fields or by adding a new field,
# it is a good idea to upgrade the version of the class, rather than sticking to the global versioning
# that is specified in the hazelcast.xml file.


# Version 2: Added new field manager name (str).
class Employee2(Portable):
    FACTORY_ID = 666
    CLASS_ID = 1
    CLASS_VERSION = 2  # specifies version different than the global version

    def __init__(self, name=None, age=None, manager=None):
        self.name = name
        self.age = age
        self.manager = manager

    def write_portable(self, writer):
        writer.write_string("name", self.name)
        writer.write_int("age", self.age)
        writer.write_string("manager", self.manager)

    def read_portable(self, reader):
        self.name = reader.read_string("name")
        self.age = reader.read_int("age")
        self.manager = reader.read_string("manager")

    def get_factory_id(self):
        return self.FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID

    # It is necessary to implement this method for multiversion support to work
    def get_class_version(self):
        return self.CLASS_VERSION

    def __str__(self):
        return "Employee(name:%s, age:%s, manager:%s)" % (self.name, self.age, self.manager)

    def __eq__(self, other):
        return (
            isinstance(other, Employee2)
            and self.name == other.name
            and self.age == other.age
            and self.manager == other.manager
        )


# However, having a version that changes across incompatible field types such as int and String will cause
# a type error as members with older versions of the class tries to access it. We will demonstrate this below.

# Version3: Changed age field type from int to String. (Incompatible type change)
class Employee3(Portable):
    FACTORY_ID = 666
    CLASS_ID = 1
    CLASS_VERSION = 3

    def __init__(self, name=None, age=None, manager=None):
        self.name = name
        self.age = age
        self.manager = manager

    def write_portable(self, writer):
        writer.write_string("name", self.name)
        writer.write_string("age", self.age)
        writer.write_string("manager", self.manager)

    def read_portable(self, reader):
        self.name = reader.read_string("name")
        self.age = reader.read_string("age")
        self.manager = reader.read_string("manager")

    def get_factory_id(self):
        return self.FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID

    def get_class_version(self):
        return self.CLASS_VERSION

    def __str__(self):
        return "Employee(name:%s, age:%s manager:%s)" % (self.name, self.age, self.manager)

    def __eq__(self, other):
        return (
            isinstance(other, Employee3)
            and self.name == other.name
            and self.age == other.age
            and self.manager == other.manager
        )


# Let's now configure 3 clients with 3 different versions of Employee.
client = hazelcast.HazelcastClient(
    portable_factories={Employee.FACTORY_ID: {Employee.CLASS_ID: Employee}}
)

client2 = hazelcast.HazelcastClient(
    portable_factories={Employee2.FACTORY_ID: {Employee2.CLASS_ID: Employee2}}
)

client3 = hazelcast.HazelcastClient(
    portable_factories={Employee3.FACTORY_ID: {Employee3.CLASS_ID: Employee3}}
)

# Assume that a member joins a cluster with a newer version of a class.
# If you modified the class by adding a new field, the new member's put operations include that
# new field.
my_map = client.get_map("employee-map").blocking()
my_map2 = client2.get_map("employee-map").blocking()

my_map.clear()
my_map.put(0, Employee("Jack", 28))
my_map2.put(1, Employee2("Jane", 29, "Josh"))

print("Map Size: %s" % my_map.size())

# If this new member tries to get an object that was put from the older members, it
# gets null for the newly added field.
for v in my_map.values():
    print(v)

for v in my_map2.values():
    print(v)

# Let's try now to put a version 3 Employee object to the map and see what happens.
my_map3 = client3.get_map("employee-map").blocking()
my_map3.put(2, Employee3("Joe", "30", "Mary"))

print("Map Size: %s" % my_map.size())

# As clients with incompatible versions of the class try to access each other, a HazelcastSerializationError
# is raised (caused by a TypeError).
try:
    # Client that has class with int type age field tries to read Employee3 object with String age field.
    print(my_map.get(2))
except HazelcastSerializationError as ex:
    print("Failed due to: %s" % ex)

try:
    # Client that has class with String type age field tries to read Employee object with int age field.
    print(my_map3.get(0))
except HazelcastSerializationError as ex:
    print("Failed due to: %s" % ex)

client.shutdown()
client2.shutdown()
client3.shutdown()
