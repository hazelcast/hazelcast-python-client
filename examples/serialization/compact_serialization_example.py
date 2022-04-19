from hazelcast import HazelcastClient
from hazelcast.serialization.api import CompactSerializer, CompactWriter, CompactReader, CompactSerializableClass


class Address:
    def __init__(self, city: str, street: str):
        self.city = city
        self.street = street

    def __repr__(self):
        return f"Address(city='{self.city}', street='{self.street}')"


class Employee:
    def __init__(self, name: str, age: int, address: Address):
        self.name = name
        self.age = age
        self.address = address

    def __repr__(self):
        return f"Employee(name='{self.name}', age={self.age}, address={self.address})"


class AddressSerializer(CompactSerializer[Address]):
    def read(self, reader: CompactReader) -> Address:
        city = reader.read_string("city")
        street = reader.read_string("street")
        return Address(city, street)

    def write(self, writer: CompactWriter, obj: Address) -> None:
        writer.write_string("city", obj.city)
        writer.write_string("street", obj.street)

    def get_type_name(self) -> str:
        return "Address"

    def get_class(self) -> CompactSerializableClass:
        return Address


class EmployeeSerializer(CompactSerializer[Employee]):
    def read(self, reader: CompactReader) -> Employee:
        name = reader.read_string("name")
        age = reader.read_int32("age")
        address = reader.read_compact("address")
        return Employee(name, age, address)

    def write(self, writer: CompactWriter, obj: Employee) -> None:
        writer.write_string("name", obj.name)
        writer.write_int32("age", obj.age)
        writer.write_compact("address", obj.address)

    def get_type_name(self) -> str:
        return "Employee"

    def get_class(self) -> CompactSerializableClass:
        return Employee


client = HazelcastClient(
    compact_serializers=[AddressSerializer(), EmployeeSerializer()]
)

employees = client.get_map("employees").blocking()

employees.set(
    0,
    Employee(
        name="John Doe",
        age=42,
        address=Address(
            city="Cambridge",
            street="3487 Cedar Lane",
        ),
    ),
)

employee = employees.get(0)

print(employee)

client.shutdown()
