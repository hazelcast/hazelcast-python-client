import logging
import hazelcast

logging.basicConfig(level=logging.INFO)

client = hazelcast.HazelcastClient()

multi_map = client.get_multi_map("multi_map").blocking()

multi_map.put("key1", "value1")
multi_map.put("key1", "value2")
multi_map.put("key2", "value3")
multi_map.put("key3", "value4")

value = multi_map.get("key1")
print(f"Get: {value}")

values = multi_map.values()
print(f"Values: {values}")

key_set = multi_map.key_set()
print(f"Key Set: {key_set}")

size = multi_map.size()
print(f"Size: {size}")

for key, value in multi_map.entry_set():
    print(f"{key} -> {value}")

client.shutdown()
