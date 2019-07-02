import time
import hazelcast

from hazelcast.serialization.api import Portable

# Default (version 1) Employee class.
class Employee(Portable):
    FACTORY_ID = 666
    CLASS_ID = 1

    def __init__(self, name=None, age=None):
        self.name = name
        self.age = age

    def write_portable(self, writer):
        writer.write_utf("name", self.name)
        writer.write_int("age", self.age)

    def read_portable(self, reader):
        self.name = reader.read_utf("name")
        self.age = reader.read_int("age")

    def get_factory_id(self):
        return self.FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID

    def __str__(self):
        return "Employee[ name:{} age:{} ]".format(self.name, self.age)

    def __eq__(self, other):
        return type(self) == type(other) and self.name == other.name and self.age == other.age

# Version 2: Added new field manager name.
class Employee2(Portable):
    FACTORY_ID = 666
    CLASS_ID = 1
    CLASS_VERSION = 2

    def __init__(self, name=None, age=None, manager=None):
        self.name = name
        self.age = age
        self.manager = manager

    def write_portable(self, writer):
        writer.write_utf("name", self.name)
        writer.write_int("age", self.age)
        writer.write_utf("manager", self.manager)

    def read_portable(self, reader):
        self.name = reader.read_utf("name")
        self.age = reader.read_int("age")
        self.manager = reader.read_utf("manager")

    def get_factory_id(self):
        return self.FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID

    def get_class_version(self):
        return self.CLASS_VERSION

    def __str__(self):
        return "Employee[ name:{} age:{} manager:{} ]".format(self.name, self.age, self.manager)

    def __eq__(self, other):
        return type(self) == type(other) and self.name == other.name and self.age == other.age \
               and self.manager == other.manager

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
        writer.write_utf("name", self.name)
        writer.write_utf("age", self.age)
        writer.write_utf("manager", self.manager)

    def read_portable(self, reader):
        self.name = reader.read_utf("name")
        self.age = reader.read_utf("age")
        self.manager = reader.read_utf("manager")

    def get_factory_id(self):
        return self.FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID

    def get_class_version(self):
        return self.CLASS_VERSION

    def __str__(self):
        return "Employee[ name:{} age:{} manager:{} ]".format(self.name, self.age, self.manager)

    def __eq__(self, other):
        return type(self) == type(other) and self.name == other.name and self.age == other.age \
               and self.manager == other.manager


if __name__ == '__main__':

    config = hazelcast.ClientConfig()
    config.serialization_config.portable_factories[Employee.FACTORY_ID] = \
        {Employee.CLASS_ID: Employee}
    client = hazelcast.HazelcastClient(config)

    config2 = hazelcast.ClientConfig()
    config2.serialization_config.portable_factories[Employee2.FACTORY_ID] = \
        {Employee2.CLASS_ID: Employee2}
    client2 = hazelcast.HazelcastClient(config2)

    config3 = hazelcast.ClientConfig()
    config3.serialization_config.portable_factories[Employee3.FACTORY_ID] = \
        {Employee3.CLASS_ID: Employee3}
    client3 = hazelcast.HazelcastClient(config3)

    my_map = client.get_map("employee-map").blocking()
    my_map2 = client2.get_map("employee-map").blocking()
    my_map3 = client3.get_map("employee-map").blocking()

    my_map.clear()
    my_map.put(0, Employee("Jack", 28))
    my_map2.put(1, Employee2("Jane", 29, "Josh"))

    print('Map Size: {}'.format(my_map.size()))

    for v in my_map.values():
        print(v)

    for v in my_map2.values():
        print(v)

    my_map3.put(2, Employee3("Joe", "30", "Mary"))

    print('Map Size: {}'.format(my_map.size()))

    try:
        print(my_map.get(2))
    except hazelcast.exception.HazelcastSerializationError:
        print("Incompatible class change. Raised TypeError while reading value with key 2 from my_map.")

    try:
        print(my_map3.get(0))
    except hazelcast.exception.HazelcastSerializationError:
        print("Incompatible class change. Raised TypeError while reading value with key 0 from my_map3.")


    time.sleep(10)
    client.shutdown()
    client2.shutdown()
    client3.shutdown()