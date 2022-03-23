import hazelcast

from hazelcast.core import HazelcastJsonValue

client = hazelcast.HazelcastClient()
employees = client.get_map("employees").blocking()

# Populate some data
employees.put(0, HazelcastJsonValue('{"name": "Alice", "age": 32}'))
employees.put(1, HazelcastJsonValue('{"name": "John", "age": 42}'))
employees.put(2, HazelcastJsonValue('{"name": "Jake", "age": 18}'))

# Create mapping for the employees map. This needs to be done only once per map.
client.sql.execute(
    """
CREATE OR REPLACE MAPPING employees
TYPE IMap
OPTIONS (
    'keyFormat' = 'int',
    'valueFormat' = 'json'
)
    """
).result()

# Select the names of employees older than 25
result = client.sql.execute(
    """
SELECT JSON_VALUE(this, '$.name') AS name
FROM employees
WHERE JSON_VALUE(this, '$.age' RETURNING INT) > 25
    """
).result()

for row in result:
    print(f"Name: {row['name']}")
