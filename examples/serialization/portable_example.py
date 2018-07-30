import hazelcast
import logging

from hazelcast.serialization.api import Portable


class Engineer(Portable):
    CLASS_ID = 1
    FACTORY_ID = 1

    def __init__(self, name=None, age=None, languages=None):
        self.name = name
        self.age = age
        self.languages = languages

    def read_portable(self, reader):
        self.name = reader.read_utf("name")
        self.age = reader.read_int("age")
        self.languages = reader.read_utf_array("languages")

    def write_portable(self, writer):
        writer.write_utf("name", self.name)
        writer.write_int("age", self.age)
        writer.write_utf_array("languages", self.languages)

    def get_factory_id(self):
        return self.FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID


if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    config = hazelcast.ClientConfig()
    factory = {Engineer.CLASS_ID: Engineer}
    config.serialization_config.add_portable_factory(Engineer.FACTORY_ID, factory)

    client = hazelcast.HazelcastClient(config)

    my_map = client.get_map("map")

    engineer = Engineer("John Doe", 30, ["Python", "Java", "C#", "C++", "Node.js", "Go"])

    my_map.put("engineer1", engineer)

    returned_engineer = my_map.get("engineer1").result()

    print("Name: {}\nAge: {}\nLanguages: {}".format(returned_engineer.name,
                                                    returned_engineer.age,
                                                    returned_engineer.languages))

    client.shutdown()
