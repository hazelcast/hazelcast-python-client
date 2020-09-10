import hazelcast
from hazelcast.config import SSLProtocol

# To use SSLConfig with mutual authentication, Hazelcast server should be started with
# SSL and mutual authentication enabled

# Start a new Hazelcast client with SSL configuration.
client = hazelcast.HazelcastClient(cluster_members=["foo.bar.com:8888"],
                                   ssl_enable=True,
                                   # Absolute paths of PEM files must be given
                                   ssl_cafile="/path/of/server.pem",
                                   ssl_certfile="/path/of/client.pem",
                                   ssl_keyfile="/path/of/client-private.pem",
                                   # If private key is not password protected, skip the option below.
                                   ssl_password="ssl_keyfile_password",
                                   # Select the protocol used in SSL communication.
                                   # This step is optional. Default is TLSv1_2
                                   ssl_protocol=SSLProtocol.TLSv1_3)

hz_map = client.get_map("ssl-map").blocking()
hz_map.put("key", "value")

print(hz_map.get("key"))

client.shutdown()
