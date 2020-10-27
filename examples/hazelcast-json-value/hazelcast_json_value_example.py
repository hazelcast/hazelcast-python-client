import hazelcast

from hazelcast.core import HazelcastJsonValue
from hazelcast.predicate import and_, greater_than, sql

client = hazelcast.HazelcastClient()
employees_map = client.get_map("employees").blocking()

alice = "{\"name\": \"Alice\", \"age\": 35}"
andy = "{\"name\": \"Andy\", \"age\": 22}"
bob = {"name": "Bob", "age": 37}

# HazelcastJsonValue can be constructed from JSON strings
employees_map.put(0, HazelcastJsonValue(alice))
employees_map.put(1, HazelcastJsonValue(andy))

# or from JSON serializable objects
employees_map.put(2, HazelcastJsonValue(bob))

# Employees whose name starts with 'A' and age is greater than 30
predicate = and_(sql("name like A%"), greater_than("age", 30))

values = employees_map.values(predicate)

for value in values:
    print(value.to_string())  # As JSON string
    print(value.loads())  # As Python object

client.shutdown()
