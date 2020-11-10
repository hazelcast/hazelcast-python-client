import hazelcast

from hazelcast.serialization.api import Portable
from hazelcast.predicate import sql, and_, between, equal


class User(Portable):
    FACTORY_ID = 1
    CLASS_ID = 1

    def __init__(self, username=None, age=None, active=None):
        self.username = username
        self.age = age
        self.active = active

    def write_portable(self, writer):
        writer.write_utf("username", self.username)
        writer.write_int("age", self.age)
        writer.write_boolean("active", self.active)

    def read_portable(self, reader):
        self.username = reader.read_utf("username")
        self.age = reader.read_int("age")
        self.active = reader.read_boolean("active")

    def get_factory_id(self):
        return self.FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID

    def __repr__(self):
        return "User(username=%s, age=%s, active=%s]" % (self.username, self.age, self.active)


def generate_users(users):
    users.put("Rod", User("Rod", 19, True))
    users.put("Jane", User("Jane", 20, True))
    users.put("Freddy", User("Freddy", 23, True))


# Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
hz = hazelcast.HazelcastClient(portable_factories={
    User.FACTORY_ID: {
        User.CLASS_ID: User
    }
})
# Get a Distributed Map called "users"
users_map = hz.get_map("users").blocking()
# Add some users to the Distributed Map
generate_users(users_map)
# Create a Predicate from a String (a SQL like Where clause)
sql_query = sql("active AND age BETWEEN 18 AND 21)")
# Creating the same Predicate as above but with a builder
criteria_query = and_(equal("active", True), between("age", 18, 21))
# Get result collections using the two different Predicates
result1 = users_map.values(sql_query)
result2 = users_map.values(criteria_query)
# Print out the results
print(result1)
print(result2)
# Shutdown this Hazelcast Client
hz.shutdown()
