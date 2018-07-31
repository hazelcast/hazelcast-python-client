import logging
import os
import hazelcast

# Hazelcast server should be started with SSL and mutual authentication enabled
# to use SSLConfig with mutual authentication
if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    config = hazelcast.ClientConfig()

    # SSL Config
    ssl_config = hazelcast.SSLConfig()
    ssl_config.enabled = True

    # Absolute path of PEM files should be given
    ssl_config.cafile = os.path.abspath("server.pem")

    # To use mutual authentication client certificate and private key should be provided
    ssl_config.certfile = os.path.abspath("client.pem")
    ssl_config.keyfile = os.path.abspath("client-key.pem")

    # If private key file is encrypted, password is required to decrypt it
    ssl_config.password = "key-file-password"

    ssl_config.hostname = "foo.bar.com"

    config.network_config.ssl_config = ssl_config

    # Start a new Hazelcast client with SSL configuration.
    client = hazelcast.HazelcastClient(config)

    hz_map = client.get_map("ssl-map")
    hz_map.put("key", "value")

    print(hz_map.get("key").result())

    client.shutdown()
