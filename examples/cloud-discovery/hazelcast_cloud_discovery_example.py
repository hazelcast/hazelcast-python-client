import hazelcast

client = hazelcast.HazelcastClient(
    # Set up cluster name for authentication
    cluster_name="vprod",
    # Set the token of your cloud cluster
    cloud_discovery_token="c1rZO25ftRaQknBqMgWF3yhdsvNriFMgsjMrFPNWYPNdvx3U2Z",
    # If you have enabled encryption for your cluster, also configure TLS/SSL for the client.
    # Otherwise, skip options below.
    ssl_enabled=True,
    ssl_cafile="/dev/shm/ssl/ca.pem",
    ssl_certfile="/dev/shm/ssl/cert.pem",
    ssl_keyfile="/dev/shm/ssl/key.pem",
    ssl_password="1c2670d289d",
)

my_map = client.get_map("map-on-the-cloud").blocking()
my_map.put("key", "value")

print(my_map.get("key"))

client.shutdown()
