import hazelcast

from hazelcast.serialization.api import Portable


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
        return "Engineer(name=%s, age=%s, languages=%s)" % (self.name, self.age, self.languages)


client = hazelcast.HazelcastClient(
    portable_factories={Engineer.FACTORY_ID: {Engineer.CLASS_ID: Engineer}}
)

my_map = client.get_map("map")

engineer = Engineer("John Doe", 30, ["Python", "Java", "C#", "C++", "Node.js", "Go"])

my_map.put("engineer1", engineer)

returned_engineer = my_map.get("engineer1").result()

print("Received", returned_engineer)

client.shutdown()
