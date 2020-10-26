import hazelcast

client = hazelcast.HazelcastClient()

my_ref = client.cp_subsystem.get_atomic_reference("my-ref").blocking()
my_ref.set(42)

value = my_ref.get()
print("Value:", value)

result = my_ref.compare_and_set(42, "value")
print("CAS result:", result)

final_value = my_ref.get()
print("Final value:", final_value)

client.shutdown()
