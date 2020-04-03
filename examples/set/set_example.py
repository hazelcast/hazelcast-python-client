import hazelcast

if __name__ == "__main__":
    config = hazelcast.ClientConfig()
    config.network_config.addresses.append("127.0.0.1:5701")
    #config.network_config.smart_routing = False
    client = hazelcast.HazelcastClient(config)
    my_set = client.get_set("set")

    my_set.add("Item1")
    my_set.add("Item1")
    my_set.add("Item2")

    found = my_set.contains("Item2").result()
    print("Set contains Item2: {}".format(found))

    items = my_set.get_all().result()
    print("Size of set: {}".format(len(items)))

    print("\nAll Items:")
    for item in items:
        print(item)

    client.shutdown()
