import logging
import os
import hazelcast

# Hazelcast server should be started with SSL enabled to use SSLConfig
if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    config = hazelcast.ClientConfig()

    # SSL Config
    ssl_config = hazelcast.SSLConfig()
    ssl_config.enabled = True

    # Absolute path of PEM file should be given
    ssl_config.cafile = os.path.abspath("server.pem")

    ssl_config.hostname = "foo.bar.com"

    config.network_config.ssl_config = ssl_config

    # Start a new Hazelcast client with SSL configuration.
    client = hazelcast.HazelcastClient(config)

    hz_map = client.get_map("ssl-map")
    hz_map.put("key", "value")

    print(hz_map.get("key").result())

    client.shutdown()
