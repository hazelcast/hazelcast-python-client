from time import sleep
import random
import hazelcast
import logging

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
    logging.getLogger().setLevel(logging.INFO)
    logger = logging.getLogger("main")

    config = hazelcast.Config()
    config.username = "dev"
    config.password = "dev-pass"
    config.addresses.append("127.0.0.1:5701")

    client = hazelcast.HazelcastClient(config)

    my_map = client.get_map("map")
    print(my_map)

    def item_added(event):
        print("item_added", event)

    def item_removed(event):
        print("item_removed", event)

    print(my_map.add_entry_listener(include_value=True, added=item_added, removed=item_removed))

    print("map.size", my_map.size())
    key = random.random()
    print("map.put", my_map.put(key, "value"))
    print("map.contains_key", my_map.contains_key(key))
    print("map.get", my_map.get(key))
    print("map.size", my_map.size())
    print("map.remove", my_map.remove(key))
    print("map.size", my_map.size())
    print("map.contains_key", my_map.contains_key(key))
    #
    sleep(30)
    #
