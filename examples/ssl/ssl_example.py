import logging
import os
import hazelcast
from hazelcast.config import PROTOCOL

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

    # Select the protocol used in SSL communication
    ssl_config.protocol = PROTOCOL.TLSv1_2

    # Hostname of the server can be checked against its certificate
    ssl_config.check_hostname = True

    config.network_config.ssl_config = ssl_config

    config.network_config.addresses.append("foo.bar.com:8888")

    # Start a new Hazelcast client with SSL configuration.
    client = hazelcast.HazelcastClient(config)

    hz_map = client.get_map("ssl-map")
    hz_map.put("key", "value")

    print(hz_map.get("key").result())

    client.shutdown()
