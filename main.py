from time import sleep
import hazelcast
import logging

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
    logging.getLogger().setLevel(logging.DEBUG)
    logger = logging.getLogger("main")

    config = hazelcast.Config()
    config.username = "dev"
    config.password = "dev-pass"
    config.addresses.append("127.0.0.1:5701")

    client = hazelcast.HazelcastClient(config)

    print("Creating proxy")
    my_map = client.get_map("map")
    print(my_map)
    print my_map.size()
    my_map.put("key", "value")
    print my_map.get("key")
    #
    sleep(30)
    #
