import hazelcast

client = hazelcast.HazelcastClient()

multi_map = client.get_multi_map("multi-map").blocking()

multi_map.put("key1", "value1")
multi_map.put("key1", "value2")
multi_map.put("key2", "value3")
multi_map.put("key3", "value4")

value = multi_map.get("key1")
print("Get:", value)

values = multi_map.values()
print("Values:", values)

key_set = multi_map.key_set()
print("Key Set:", key_set)

size = multi_map.size()
print("Size:", size)

for key, value in multi_map.entry_set():
    print("%s -> %s" % (key, value))

client.shutdown()
