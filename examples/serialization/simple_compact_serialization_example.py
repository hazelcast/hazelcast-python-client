from hazelcast import HazelcastClient
from hazelcast.serialization.api import CompactSerializer, CompactReader, CompactWriter


class Employee:
    def __init__(self, age: int, id: int):
        self.age = age
        self.id = id

    def __repr__(self):
        return f"Employee(age={self.age}, id={self.id})"


class EmployeeSerializer(CompactSerializer[Employee]):
    def read(self, reader: CompactReader):
        age = reader.read_int32("age")
        id = reader.read_int64("id")
        return Employee(age, id)

    def write(self, writer: CompactWriter, obj: Employee):
        writer.write_int32("age", obj.age)
        writer.write_int64("id", obj.id)

    def get_type_name(self):
        return "Employee"

    def get_class(self):
        return Employee


client = HazelcastClient(compact_serializers=[EmployeeSerializer()])

m = client.get_map("compactSerializationSampleMap").blocking()

m.put(20, Employee(1, 1))

employee = m.get(20)

print(employee)

client.shutdown()
