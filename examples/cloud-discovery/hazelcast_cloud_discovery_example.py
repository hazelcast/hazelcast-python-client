import hazelcast

config = hazelcast.ClientConfig()

# Set up cluster name for authentication
config.cluster_name.name = "YOUR_CLUSTER_NAME"

# Enable Hazelcast.Cloud configuration and set the token of your cluster.
config.network.cloud.enabled = True
config.network.cloud.discovery_token = "YOUR_CLUSTER_DISCOVERY_TOKEN"

# If you have enabled encryption for your cluster, also configure TLS/SSL for the client.
# Otherwise, skip this step.
config.network.ssl.enabled = True
config.network.ssl.cafile = "/path/to/ca.pem"
config.network.ssl.certfile = "/path/to/cert.pem"
config.network.ssl.keyfile = "/path/to/key.pem"
config.network.ssl.password = "YOUR_KEY_STORE_PASSWORD"

# Start a new Hazelcast client with this configuration.
client = hazelcast.HazelcastClient(config)

my_map = client.get_map("map-on-the-cloud").blocking()
my_map.put("key", "value")

print(my_map.get("key"))

client.shutdown()
