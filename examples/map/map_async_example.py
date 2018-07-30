import hazelcast
import logging
import random
import time


def fill_map(hz_map, count=10):
    for i in range(count):
        hz_map.put("key-" + str(i), "value-" + str(i))


if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    client = hazelcast.HazelcastClient()

    my_map = client.get_map("async-map")
    fill_map(my_map)

    print("Map size: {}".format(my_map.size().result()))

    def put_callback(future):
        print("Map put: {}".format(future.result()))

    my_map.put("key", "async-value").add_done_callback(put_callback)

    def contains_callback(future):
        print("Map contains: {}".format(future.result()))

    key = random.random()
    print("Random key: {}".format(key))
    my_map.contains_key(key).add_done_callback(contains_callback)

    time.sleep(10)
    client.shutdown()
