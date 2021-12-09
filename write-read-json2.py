# taken from: https://github.com/hazelcast/hazelcast-python-client/blob/master/examples/hazelcast-json-value/hazelcast_json_value_example.py

import hazelcast

from hazelcast.core import HazelcastJsonValue
from hazelcast.predicate import and_, greater, sql

client = hazelcast.HazelcastClient(
    cluster_name="hello-world",
)
employees_map = client.get_map("employees").blocking()

alice = '{"name": "Alice", "age": 35}'
andy = '{"name": "Andy", "age": 22}'
bob = {"name": "Bob", "age": 37}

# HazelcastJsonValue can be constructed from JSON strings
employees_map.put(0, HazelcastJsonValue(alice))
employees_map.put(1, HazelcastJsonValue(andy))

# or from JSON serializable objects
employees_map.put(2, HazelcastJsonValue(bob))

# Employees whose name starts with 'A' and age is greater than 30
# predicate = and_(sql("name like A%"), greater("age", 30))

predicate = and_(greater("age", 30))

values = employees_map.values(predicate)

for value in values:
    print("VALUE:")
    print(value.to_string())  # As JSON string
    print(value.loads())  # As Python object
