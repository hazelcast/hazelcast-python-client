import hazelcast

from hazelcast.predicate import between

client = hazelcast.HazelcastClient()

predicate_map = client.get_map("predicate-map").blocking()

for i in range(10):
    predicate_map.put("key" + str(i), i)

predicate = between("this", 3, 5)

predicate_map.remove_all(predicate)

print(predicate_map.values())

client.shutdown()
