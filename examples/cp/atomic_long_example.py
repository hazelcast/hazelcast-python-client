import logging
import hazelcast

logging.basicConfig(level=logging.INFO)

client = hazelcast.HazelcastClient()

view_counter = client.cp_subsystem.get_atomic_long("views").blocking()

value = view_counter.get()
print(f"Value: {value}")

new_value = view_counter.add_and_get(42)
print(f"New value: {new_value}")

client.shutdown()
