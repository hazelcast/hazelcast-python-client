import logging

import hazelcast

if __name__ == "__main__":
    # Connect
    config = hazelcast.ClientConfig()
    connection_retry_config = config.connection_strategy_config.connection_retry_config
    config.connection_strategy_config.async_start = False
    """"""
    connection_retry_config.set_initial_backoff_millis(1000)\
        .set_max_backoff_millis(60000).set_multiplier(2)\
        .set_cluster_connect_timeout_millis(10000).set_jitter(0.2)


    config.logger_config.level = logging.DEBUG
    #config.network_config.addresses.append("192.168.1.29:5701")
    #config.network_config.smart_routing = False
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
