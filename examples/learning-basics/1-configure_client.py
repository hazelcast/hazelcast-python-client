import hazelcast

# Create configuration for the client
config = hazelcast.ClientConfig()
print("Cluster name: {}".format(config.cluster_name))

# Add member's host:port to the configuration.
# For each member on your Hazelcast cluster, you should add its host:port pair to the configuration.
config.network.addresses.append("127.0.0.1:5701")
config.network.addresses.append("127.0.0.1:5702")

# Create a client using the configuration above
client = hazelcast.HazelcastClient(config)

# Disconnect the client and shutdown
client.shutdown()
