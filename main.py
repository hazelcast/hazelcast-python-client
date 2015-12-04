from time import sleep
import hazelcast

if __name__ == '__main__':

    config = hazelcast.Config()
    config.username = "dev"
    config.password = "dev-pass"
    config.add_address("127.0.0.1:5701")

    client = hazelcast.HazelcastClient(config)

    #map = client.get_map("map")
    #map.put("key", "value")
    # print(map.get("key"))

    sleep(30)
