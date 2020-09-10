import hazelcast

# Create a client using the configuration below
client = hazelcast.HazelcastClient(
    # Add member's host:port to the configuration.
    # For each member on your Hazelcast cluster, you should add its host:port pair to the configuration.
    # If the port is not specified, by default 5701, 5702 and 5703 will be tried.
    cluster_members=[
        "127.0.0.1:5701",
        "127.0.0.1:5702",
    ]
)

# Disconnect the client and shutdown
client.shutdown()
