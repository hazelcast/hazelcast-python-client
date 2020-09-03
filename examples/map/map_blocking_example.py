import hazelcast
import random
import time


def fill_map(hz_map, count=10):
    entries = {"key-" + str(i): "value-" + str(i) for i in range(count)}
    hz_map.put_all(entries)


client = hazelcast.HazelcastClient()

my_map = client.get_map("sync-map").blocking()
fill_map(my_map)

print("Map size: {}".format(my_map.size()))

random_key = random.random()
my_map.put(random_key, "value")
print("Map contains {}: {}".format(random_key, my_map.contains_key(random_key)))
print("Map size: {}".format(my_map.size()))

my_map.remove(random_key)
print("Map contains {}: {}".format(random_key, my_map.contains_key(random_key)))
print("Map size: {}".format(my_map.size()))

print("\nIterate over the map\n")

for key, value in my_map.entry_set():
    print("Key: {} -> Value: {}".format(key, value))

client.shutdown()
