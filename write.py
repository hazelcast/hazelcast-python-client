import hazelcast

if __name__ == "__main__":
    client = hazelcast.HazelcastClient(
        cluster_name="hello-world",
    )

    # Create a Distributed Map in the cluster
    map = client.get_map("my-distributed-map").blocking()

    map.put("1", "John")
    map.put("2", "Mary")
    map.put("3", "Jane")
