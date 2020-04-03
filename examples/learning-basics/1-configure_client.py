import hazelcast

if __name__ == "__main__":
    # Create configuration for the client
    config = hazelcast.ClientConfig()
    print("Cluster name: {}".format(config.group_config.name))
    #config.network_config.smart_routing = False
    # Add member's host:port to the configuration.
    # For each member on your Hazelcast cluster, you should add its host:port pair to the configuration.
    #config.network_config.addresses.append("10.216.1.42:5701")


    # Create a client using the configuration above
    client = hazelcast.HazelcastClient(config)
    print("Client is {}".format(client.lifecycle.state))

    # Disconnect the client and shutdown
    client.shutdown()
