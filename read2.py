import hazelcast

if __name__ == "__main__":
    client = hazelcast.HazelcastClient(
        cluster_name="hello-world",
    )

    my_map = client.get_map("capitals").blocking()
    for key, value in my_map.entry_set():
        print(key, value)

    client.shutdown()
