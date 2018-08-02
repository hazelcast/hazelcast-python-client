import hazelcast
import logging

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    # Create configuration for the client
    config = hazelcast.ClientConfig()
    print("Cluster name: {}".format(config.group_config.name))

    # Add member's host:port to the configuration.
    # For each member on your Hazelcast cluster, you should add its host:port pair to the configuration.
    config.network_config.addresses.append("127.0.0.1:5701")
    config.network_config.addresses.append("127.0.0.1:5702")

    # Create a client using the configuration above
    client = hazelcast.HazelcastClient(config)
    print("Client is {}".format(client.lifecycle.state))

    # Disconnect the client and shutdown
    client.shutdown()
