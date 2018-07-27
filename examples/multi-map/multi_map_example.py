import hazelcast
import logging

if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    client = hazelcast.HazelcastClient()

    multi_map = client.get_multi_map("multi-map")

    multi_map.put("key1", "value1")
    multi_map.put("key1", "value2")
    multi_map.put("key2", "value3")
    multi_map.put("key3", "value4")

    value = multi_map.get("key1").result()
    print("Get: {}".format(value))

    values = multi_map.values().result()
    print("Values: {}".format(values))

    key_set = multi_map.key_set().result()
    print("Key Set: {}".format(key_set))

    size = multi_map.size().result()
    print("Size: {}".format(size))

    for key, value in multi_map.entry_set().result():
        print("{} -> {}".format(key, value))

    client.shutdown()
