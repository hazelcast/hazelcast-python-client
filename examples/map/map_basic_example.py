import hazelcast

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

    print("Entry with key 3: {}".format(my_map.get("3").result()))

    print("Map size: {}".format(my_map.size().result()))

    # Print the map
    print("\nIterating over the map: \n")

    entries = my_map.entry_set().result()
    for key, value in entries:
        print("{} -> {}".format(key, value))

    client.shutdown()
