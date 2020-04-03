import hazelcast

if __name__ == "__main__":
    # Connect
    config = hazelcast.ClientConfig()
    #config.network_config.addresses.append("192.168.1.22:5703")
    #config.network_config.smart_routing = True
    client = hazelcast.HazelcastClient(config)

    # We can access maps on the server from the client. Let's access the greetings map that we created already
    greetings_map = client.get_map("greetings-map")

    # Get the keys of the map
    keys = greetings_map.key_set().result()
    print(greetings_map.get("Turkish").result())
    # Print key-value pairs
    for key in keys:
        print("{} -> {}".format(key, greetings_map.get(key).result()))

    # Shutdown the client
    client.shutdown()
