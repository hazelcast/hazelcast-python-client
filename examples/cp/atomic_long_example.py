import hazelcast

client = hazelcast.HazelcastClient()

view_counter = client.cp_subsystem.get_atomic_long("views").blocking()
value = view_counter.get()
print("Value:", value)
new_value = view_counter.add_and_get(42)
print("New value:", new_value)

client.shutdown()
