import hazelcast
import threading

if __name__ == "__main__":

    config = hazelcast.ClientConfig()
    config.network_config.addresses.append("127.0.0.1:5701")
    #config.network_config.smart_routing = False
    client = hazelcast.HazelcastClient(config)

    my_map = client.get_map("my-map")

    # Fill the map
    my_map.put("1", "Tokyo")
    my_map.put("2", "Paris")
    my_map.put("3", "Istanbul")

    my_map.lock("2")

    print(my_map.is_locked("2").result())

    t = threading.Thread(target=lambda: my_map.force_unlock("2"))
    t.start()

    print(my_map.is_locked("2").result())
    t.join()
    client.shutdown()
