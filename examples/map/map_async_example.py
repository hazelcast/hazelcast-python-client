import hazelcast
import random
import time


def fill_map(hz_map, count=10):
    entries = {"key-" + str(i): "value-" + str(i) for i in range(count)}
    hz_map.put_all(entries)


def put_callback(future):
    print("Map put: {}".format(future.result()))


def contains_callback(future):
    print("Map contains: {}".format(future.result()))


client = hazelcast.HazelcastClient()

my_map = client.get_map("async-map")
fill_map(my_map)

print("Map size: {}".format(my_map.size().result()))

my_map.put("key", "async-value").add_done_callback(put_callback)

key = random.random()
print("Random key: {}".format(key))
my_map.contains_key(key).add_done_callback(contains_callback)

time.sleep(3)
client.shutdown()
