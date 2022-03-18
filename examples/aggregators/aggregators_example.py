import logging
import hazelcast

from hazelcast.aggregator import count, number_avg, max_by
from hazelcast.predicate import less_or_equal

logging.basicConfig(level=logging.INFO)

client = hazelcast.HazelcastClient()

people = client.get_map("people").blocking()

people.put_all(
    {
        "Philip": 46,
        "Elizabeth": 44,
        "Henry": 13,
        "Paige": 15,
    }
)

people_count = people.aggregate(count())
print(f"There are {people_count} people.")

children_count = people.aggregate(count(), less_or_equal("this", 18))
print(f"There are {children_count} children.")

average_age = people.aggregate(number_avg())
print(f"Average age is {average_age}.")

eldest_person = people.aggregate(max_by("this"))
print(f"Eldest person is {eldest_person.key}, with the age of {eldest_person.value}.")

client.shutdown()
