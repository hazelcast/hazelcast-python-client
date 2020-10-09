import hazelcast

client = hazelcast.HazelcastClient(
    # Set up cluster name for authentication
    cluster_name="YOUR_CLUSTER_NAME",
    # Set the token of your cloud cluster
    cloud_discovery_token="YOUR_CLUSTER_DISCOVERY_TOKEN",
    # If you have enabled encryption for your cluster, also configure TLS/SSL for the client.
    # Otherwise, skip options below.
    ssl_enabled=True,
    ssl_cafile="/path/to/ca.pem",
    ssl_certfile="/path/to/cert.pem",
    ssl_keyfile="/path/to/key.pem",
    ssl_password="YOUR_KEY_STORE_PASSWORD"
)

my_map = client.get_map("map-on-the-cloud").blocking()
my_map.put("key", "value")

print(my_map.get("key"))

client.shutdown()
