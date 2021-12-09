import hazelcast

if __name__ == "__main__":
    hz_client = hazelcast.HazelcastClient(
        cluster_name="hello-world",
    )

    # Create a Distributed Map in the cluster
capital_cities = hz_client.get_map("capitals").blocking()
capital_cities.put(1, "Tokyo")
capital_cities.put(2, "Paris")
capital_cities.put(3, "Washington")
capital_cities.put(4, "Ankara")
capital_cities.put(5, "Brussels")
