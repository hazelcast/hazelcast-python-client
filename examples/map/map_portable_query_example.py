import logging
import hazelcast
import time

from hazelcast.serialization.api import Portable
from hazelcast.predicate import sql

logging.basicConfig(level=logging.INFO)


class Employee(Portable):
    FACTORY_ID = 666
    CLASS_ID = 2

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
        return f"Employee(name={self.name}, age={self.age})"

    def __eq__(self, other):
        return isinstance(other, Employee) and self.name == other.name and self.age == other.age


client = hazelcast.HazelcastClient(
    portable_factories={
        Employee.FACTORY_ID: {
            Employee.CLASS_ID: Employee,
        },
    },
)

employee_map = client.get_map("employee_map").blocking()

employee_map.put(0, Employee("Jack", 28))
employee_map.put(1, Employee("Jane", 29))
employee_map.put(2, Employee("Joe", 30))

map_size = employee_map.size()
print(f"Map Size: {map_size}")

predicate = sql("age <= 29")

values = employee_map.values(predicate)
for value in values:
    print(f"Value: {value}")

client.shutdown()
