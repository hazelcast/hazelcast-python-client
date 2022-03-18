import logging
import hazelcast

from hazelcast.predicate import between

logging.basicConfig(level=logging.INFO)

client = hazelcast.HazelcastClient()

predicate_map = client.get_map("predicate_map").blocking()
for i in range(10):
    predicate_map.put(f"key-{i}", i)

predicate = between("this", 3, 5)

entry_set = predicate_map.entry_set(predicate)

for key, value in entry_set:
    print(f"{key} -> {value}")

client.shutdown()
