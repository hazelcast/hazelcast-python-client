from hazelcast import HazelcastClient

client = HazelcastClient(
    cluster_name="a-cluster",
    cluster_members=["10.212.1.132:5701"],
    ssl_enabled=True,
    near_caches={
        "a-map": {
            "time_to_live": 120,
            "max_idle": 60,
        }
    },
)

# Do something with the client

client.shutdown()
