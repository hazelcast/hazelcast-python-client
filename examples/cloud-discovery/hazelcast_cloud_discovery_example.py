import hazelcast

if __name__ == "__main__":
    config = hazelcast.ClientConfig()

    # Set up group name and password for authentication
    config.group_config.name = "YOUR_CLUSTER_NAME"
    config.group_config.password = "YOUR_CLUSTER_PASSWORD"

    # Enable Hazelcast.Cloud configuration and set the token of your cluster.
    config.network_config.cloud_config.enabled = True
    config.network_config.cloud_config.discovery_token = "YOUR_CLUSTER_DISCOVERY_TOKEN"

    # If you have enabled encryption for your cluster, also configure TLS/SSL for the client.
    # Otherwise, skip this step.
    config.network_config.ssl_config.enabled = True
    config.network_config.ssl_config.cafile = "/path/to/ca.pem"
    config.network_config.ssl_config.certfile = "/path/to/cert.pem"
    config.network_config.ssl_config.keyfile = "/path/to/key.pem"
    config.network_config.ssl_config.password = "YOUR_KEY_STORE_PASSWORD"

    # Start a new Hazelcast client with this configuration.
    client = hazelcast.HazelcastClient(config)

    my_map = client.get_map("map-on-the-cloud").blocking()
    my_map.put("key", "value")

    print(my_map.get("key"))

    client.shutdown()
