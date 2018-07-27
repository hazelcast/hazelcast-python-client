import logging
import time
import hazelcast

from hazelcast.serialization.api import Portable
from hazelcast.serialization.predicate import sql


class Employee(Portable):
    FACTORY_ID = 666
    CLASS_ID = 2

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


if __name__ == '__main__':
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    config = hazelcast.ClientConfig()

    config.serialization_config.portable_factories[Employee.FACTORY_ID] = \
        {Employee.CLASS_ID: Employee}

    client = hazelcast.HazelcastClient(config)

    my_map = client.get_map("employee-map")
    #
    my_map.put(0, Employee("Jack", 28))
    my_map.put(1, Employee("Jane", 29))
    my_map.put(2, Employee("Joe", 30))

    print("Map Size: {}".format(my_map.size().result()))

    predicate = sql("age <= 29")

    def values_callback(f):
        result_set = f.result()
        print("Query Result Size: {}".format(len(result_set)))
        for value in result_set:
            print("value: {}".format(value))

    my_map.values(predicate).add_done_callback(values_callback)

    time.sleep(10)
    client.shutdown()
