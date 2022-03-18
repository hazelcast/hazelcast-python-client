import logging
import hazelcast

logging.basicConfig(level=logging.INFO)

client = hazelcast.HazelcastClient()

pn_counter = client.get_pn_counter("pn_counter").blocking()

print(f"Counter is initialized with {pn_counter.get()}")

for i in range(10):
    print(f"Added {i} to the counter. Current value is {pn_counter.add_and_get(i)}")

print(
    f"Incremented the counter after getting the current value. "
    f"Previous value is {pn_counter.get_and_increment()}"
)

print(f"Final value is {pn_counter.get()}")
