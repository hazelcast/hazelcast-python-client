import hazelcast
import logging

from hazelcast.serialization.predicate import BetweenPredicate

if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    client = hazelcast.HazelcastClient()

    predicate_map = client.get_map("predicate-map")
    for i in range(10):
        predicate_map.put("key" + str(i), i)

    predicate = BetweenPredicate("this", 3, 5)

    entry_set = predicate_map.entry_set(predicate).result()

    for key, value in entry_set:
        print("{} -> {}".format(key, value))

    client.shutdown()
