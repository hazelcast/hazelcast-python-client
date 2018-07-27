import hazelcast
import logging

from hazelcast import ClientConfig
from hazelcast.serialization.api import Portable
from hazelcast.serialization.predicate import SqlPredicate, and_, is_between, is_equal_to


class User(Portable):
    FACTORY_ID = 1
    CLASS_ID = 1

    def __init__(self, user_name=None, age=None, active=None):
        self.user_name = user_name
        self.age = age
        self.active = active

    def write_portable(self, writer):
        writer.write_utf("user_name", self.user_name)
        writer.write_int("age", self.age)
        writer.write_boolean("active", self.active)

    def read_portable(self, reader):
        self.user_name = reader.read_utf("user_name")
        self.age = reader.read_int("age")
        self.active = reader.read_boolean("active")

    def get_factory_id(self):
        return self.FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID

    def __repr__(self):
        return '%s %s %s' % (self.user_name, self.age, self.active)


def generate_users(users):
    users.put("Rod", User("Rod", 19, True))
    users.put("Jane", User("Jane", 20, True))
    users.put("Freddy", User("Freddy", 23, True))


if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
    logging.getLogger().setLevel(logging.INFO)

    # Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
    config = ClientConfig()
    portable_factory = {User.CLASS_ID: User}
    config.serialization_config.add_portable_factory(User.FACTORY_ID, portable_factory)
    hz = hazelcast.HazelcastClient(config)
    # Get a Distributed Map called "users"
    users = hz.get_map("users")
    # Add some users to the Distributed Map
    generate_users(users)
    # Create a Predicate from a String (a SQL like Where clause)
    sqlQuery = SqlPredicate("active AND age BETWEEN 18 AND 21)")
    # Creating the same Predicate as above but with a builder
    criteriaQuery = and_(is_equal_to("active", True), is_between("age", 18, 21))
    # Get result collections using the two different Predicates
    result1 = users.values(sqlQuery).result()
    result2 = users.values(criteriaQuery).result()
    # Print out the results
    print(result1)
    print(result2)
    # Shutdown this Hazelcast Client
    hz.shutdown()
