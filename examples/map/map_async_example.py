import logging
import hazelcast
import random
import time

logging.basicConfig(level=logging.INFO)

client = hazelcast.HazelcastClient()

async_map = client.get_map("async_map")

# Fill the map
entries = {f"key-{i}": f"value-{i}" for i in range(10)}
async_map.put_all(entries).result()

map_size = async_map.size().result()
print(f"Map size: {map_size}")


def put_callback(future):
    print(f"Map put result: {future.result()}")


async_map.put("key", "async-value").add_done_callback(put_callback)

key = random.randint(0, 20)
print(f"Random key: {key}")


def contains_callback(future):
    print(f"Map contains result: {future.result()}")


async_map.contains_key(f"key-{key}").add_done_callback(contains_callback)

time.sleep(3)
client.shutdown()
