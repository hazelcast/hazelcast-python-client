import hazelcast

client = hazelcast.HazelcastClient(flake_id_generators={
    "id-generator": {
        "prefetch_count": 50,
        "prefetch_validity": 30,
    }
})

generator = client.get_flake_id_generator("id-generator").blocking()

for _ in range(100):
    print("Id:", generator.new_id())

client.shutdown()
