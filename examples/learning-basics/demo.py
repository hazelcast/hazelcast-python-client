import hazelcast

if __name__ == "__main__":
    # Connect
    config = hazelcast.ClientConfig()
    config.network_config.addresses.append("127.0.0.1:5701")
    #config.network_config.smart_routing = False
    client = hazelcast.HazelcastClient(config)

    # We can access maps on the server from the client. Let's access the greetings map that we created already
    my_map = client.get_map("demo-map")
    d = {"bir": "one", "iki": "two"}
    my_map.put_all(d)
    my_map.put_if_absent("üc","three")
    print(my_map.remove("üc").result())
    my_map.replace("iki", 2)
    #my_map.clear().result()
    keys = my_map.key_set().result()
    # Print key-value pairs
    #my_map.clear()
    for key in keys:
        print("{} -> {}".format(key, my_map.get(key).result()))

    #my_map.delete("iki").result()

    for entry in my_map.entry_set().result():
        print(entry)

    print(my_map.is_empty().result())
    print(my_map.get_all(["bir", "iki"]).result())
    print(my_map.is_locked("bir").result())
    my_map.remove_if_same("bir", "one").result()
    print(my_map.values().result())
    # Shutdown the client
    client.shutdown()
