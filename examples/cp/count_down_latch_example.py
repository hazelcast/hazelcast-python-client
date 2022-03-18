import logging
import hazelcast

logging.basicConfig(level=logging.INFO)

client = hazelcast.HazelcastClient()

latch = client.cp_subsystem.get_count_down_latch("my-latch").blocking()

initial_count = latch.try_set_count(3)
print(f"Initialized with: {initial_count}")

count = latch.get_count()
print(f"Count: {count}")

await_result = latch.await_latch(10)
print(f"Result of await: {await_result}")

for _ in range(3):
    latch.count_down()
    count = latch.get_count()
    print("Current count:", count)

client.shutdown()
