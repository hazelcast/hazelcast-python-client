import dataclasses

from hazelcast import HazelcastClient
from hazelcast.serialization.api import CompactSerializer, CompactWriter, CompactReader, CompactSerializableClass


@dataclasses.dataclass
class Person:
    name: str
    age: int


class PersonSerializer(CompactSerializer[Person]):
    def read(self, reader: CompactReader) -> Person:
        name = reader.read_string("name")
        age = reader.read_int32("age")
        return Person(name, age)

    def write(self, writer: CompactWriter, obj: Person) -> None:
        writer.write_string("name", obj.name)
        writer.write_int32("age", obj.age)

    def get_type_name(self) -> str:
        return "Person"

    def get_class(self) -> CompactSerializableClass:
        return Person


client = HazelcastClient(
    compact_serializers=[PersonSerializer()]
)

client.sql.execute(
    """
CREATE MAPPING IF NOT EXISTS persons (
    __key INT,
    name VARCHAR,
    age INT
)
TYPE IMap
OPTIONS (
    'keyFormat' = 'int',
    'valueFormat' = 'compact',
    'valueCompactTypeName' = 'Person'
)
"""
).result()


persons = client.get_map("persons").blocking()
persons.set(0, Person("John Doe", 42))
persons.set(1, Person("Jane Doe", 39))

with client.sql.execute("SELECT name FROM persons WHERE age > 40").result() as rows:
    for row in rows:
        print(f"Name: {row['name']}")
