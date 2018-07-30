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

    # Get a map that is stored on the server side. We can access it from the client
    greetings_map = client.get_map("greetings-map")

    # Map is empty on the first run. It will be non-empty if Hazelcast has data on this map
    print("Map: {}, Size: {}".format(greetings_map.name, greetings_map.size().result()))

    # Write data to map. If there is a data with the same key already, it will be overwritten
    greetings_map.put("English", "hello world")
    greetings_map.put("Spanish", "hola mundo")
    greetings_map.put("Italian", "ciao mondo")
    greetings_map.put("German", "hallo welt")
    greetings_map.put("French", "bonjour monde")

    # 5 data is added to the map. There should be at least 5 data on the server side
    print("Map: {},  Size: {}".format(greetings_map.name, greetings_map.size().result()))

    # Shutdown the client
    client.shutdown()
