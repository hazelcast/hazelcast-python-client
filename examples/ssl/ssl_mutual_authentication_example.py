import os
import hazelcast
from hazelcast.config import PROTOCOL

# To use SSLConfig with mutual authentication, Hazelcast server should be started with
# SSL and mutual authentication enabled
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

# Select the protocol used in SSL communication. This step is optional. Default is TLSv1_2
ssl_config.protocol = PROTOCOL.TLSv1_3

config.network.ssl = ssl_config

config.network.addresses.append("foo.bar.com:8888")

# Start a new Hazelcast client with SSL configuration.
client = hazelcast.HazelcastClient(config)

hz_map = client.get_map("ssl-map")
hz_map.put("key", "value")

print(hz_map.get("key").result())

client.shutdown()
