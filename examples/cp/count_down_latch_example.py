import hazelcast

client = hazelcast.HazelcastClient()

latch = client.cp_subsystem.get_count_down_latch("my-latch")
initialized = latch.try_set_count(3).result()
print("Initialized:", initialized)
count = latch.get_count().result()
print("Count:", count)

latch.await_latch(10).add_done_callback(lambda f: print("Result of await:", f.result()))

for _ in range(3):
    latch.count_down().result()
    count = latch.get_count().result()
    print("Current count:", count)

client.shutdown()
