import hazelcast

if __name__ == "__main__":
    config = hazelcast.ClientConfig()

    # Set up group name and password for authentication
    config.group_config.name = "name"
    config.group_config.password = "password"

    # Enable SSL for encryption. Default CA certificates will be used.
    config.network.ssl.enabled = True

    # Enable Hazelcast.Cloud configuration and set the token of your cluster.
    config.network.cloud.enabled = True
    config.network.cloud.discovery_token = "token"

    # Start a new Hazelcast client with this configuration.
    client = hazelcast.HazelcastClient(config)

    my_map = client.get_map("map-on-the-cloud")
    my_map.put("key", "hazelcast.cloud")

    print(my_map.get("key"))

    client.shutdown()
