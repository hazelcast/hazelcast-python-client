import hazelcast
import logging
import os

if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    config = hazelcast.ClientConfig()

    ssl_config = hazelcast.SSLConfig()
    ssl_config.enabled = True

    # PEM file paths should be absolute
    ssl_config.cafile = os.path.abspath("server.pem")
    ssl_config.certfile = os.path.abspath("client.pem")
    ssl_config.keyfile = os.path.abspath("client-key.pem")
    ssl_config.password = "keyfile-password"

    config.network_config.ssl_config = ssl_config

    cloud_config = hazelcast.ClientCloudConfig()
    cloud_config.enabled = True
    cloud_config.discovery_token = "token"

    config.network_config.cloud_config = cloud_config

    client = hazelcast.HazelcastClient(config)

    my_map = client.get_map("map-on-the-cloud")
    my_map.put("key", "hazelcast.cloud")

    print(my_map.get("key"))

    client.shutdown()
