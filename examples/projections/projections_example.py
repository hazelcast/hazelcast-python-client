import hazelcast

from hazelcast.core import HazelcastJsonValue
from hazelcast.predicate import less_or_equal
from hazelcast.projection import single_attribute, multi_attribute

client = hazelcast.HazelcastClient()

people = client.get_map("people").blocking()

people.put_all(
    {
        1: HazelcastJsonValue({"name": "Philip", "age": 46}),
        2: HazelcastJsonValue({"name": "Elizabeth", "age": 44}),
        3: HazelcastJsonValue({"name": "Henry", "age": 13}),
        4: HazelcastJsonValue({"name": "Paige", "age": 15}),
    }
)

names = people.project(single_attribute("name"))
print("Names of the people are %s." % names)

children_names = people.project(single_attribute("name"), less_or_equal("age", 18))
print("Names of the children are %s." % children_names)

names_and_ages = people.project(multi_attribute("name", "age"))
print("Names and ages of the people are %s." % names_and_ages)

client.shutdown()
