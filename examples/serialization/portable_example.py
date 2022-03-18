import logging
import hazelcast

from hazelcast.serialization.api import Portable

logging.basicConfig(level=logging.INFO)


class Engineer(Portable):
    CLASS_ID = 1
    FACTORY_ID = 1

    def __init__(self, name=None, age=None, languages=None):
        self.name = name
        self.age = age
        self.languages = languages

    def read_portable(self, reader):
        self.name = reader.read_string("name")
        self.age = reader.read_int("age")
        self.languages = reader.read_string_array("languages")

    def write_portable(self, writer):
        writer.write_string("name", self.name)
        writer.write_int("age", self.age)
        writer.write_string_array("languages", self.languages)

    def get_factory_id(self):
        return self.FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID

    def __repr__(self):
        return f"Engineer(name={self.name}, age={self.age}, languages={self.languages})"


client = hazelcast.HazelcastClient(
    portable_factories={
        Engineer.FACTORY_ID: {
            Engineer.CLASS_ID: Engineer,
        },
    },
)

engineers = client.get_map("engineers").blocking()

engineer = Engineer(
    "John Doe",
    30,
    ["Python", "Java", "C#", "C++", "Node.js", "Go"],
)

engineers.put("engineer1", engineer)

returned_engineer = engineers.get("engineer1")
print(f"Received {returned_engineer}")

client.shutdown()
