import hazelcast
import logging

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    # Connect
    config = hazelcast.ClientConfig()
    config.network_config.addresses.append("127.0.0.1:5701")
    client = hazelcast.HazelcastClient(config)

    # We can access maps on the server from the client. Let's access the greetings map that we created already
    greetings_map = client.get_map("greetings-map")

    # Get the keys of the map
    keys = greetings_map.key_set().result()

    # Print key-value pairs
    for key in keys:
        print("{} -> {}".format(key, greetings_map.get(key).result()))

    # Shutdown the client
    client.shutdown()
