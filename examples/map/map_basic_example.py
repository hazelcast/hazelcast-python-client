import hazelcast
import logging

if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    client = hazelcast.HazelcastClient()

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
