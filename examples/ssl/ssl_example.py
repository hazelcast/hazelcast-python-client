import logging
import hazelcast

from hazelcast.config import SSLProtocol

logging.basicConfig(level=logging.INFO)

# Hazelcast server should be started with SSL enabled

# Start a new Hazelcast client with SSL configuration.
client = hazelcast.HazelcastClient(
    cluster_members=["foo.bar.com:8888"],
    ssl_enabled=True,
    # Absolute paths of PEM files must be given
    ssl_cafile="/path/of/server.pem",
    # Select the protocol used in SSL communication.
    # This step is optional. Default is TLSv1_2
    ssl_protocol=SSLProtocol.TLSv1_3,
)

ssl_map = client.get_map("ssl_map").blocking()
ssl_map.put("key", "value")

print(ssl_map.get("key"))

client.shutdown()
