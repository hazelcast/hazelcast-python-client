import hazelcast

from hazelcast.aggregator import count, number_avg, max_by
from hazelcast.predicate import less_or_equal

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
print("There are %d people." % people_count)

children_count = people.aggregate(count(), less_or_equal("this", 18))
print("There are %d children." % children_count)

average_age = people.aggregate(number_avg())
print("Average age is %f." % average_age)

eldest_person = people.aggregate(max_by("this"))
print("Eldest person is %s, with the age of %d." % (eldest_person.key, eldest_person.value))

client.shutdown()
