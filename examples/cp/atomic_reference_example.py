import logging
import hazelcast

logging.basicConfig(level=logging.INFO)

client = hazelcast.HazelcastClient()

my_ref = client.cp_subsystem.get_atomic_reference("my-ref").blocking()
my_ref.set(42)

value = my_ref.get()
print(f"Value: {value}")

result = my_ref.compare_and_set(42, "value")
print(f"CAS result: {result}")

final_value = my_ref.get()
print(f"Final value: {final_value}")

client.shutdown()
