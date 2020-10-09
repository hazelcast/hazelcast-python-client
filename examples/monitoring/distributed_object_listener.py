import hazelcast


def distributed_object_listener(event):
    print("Distributed object event >>>", event.name, event.service_name, event.event_type)


client = hazelcast.HazelcastClient()

# Register the listener
reg_id = client.add_distributed_object_listener(distributed_object_listener)

map_name = "test_map"

# This call causes a CREATED event
test_map = client.get_map(map_name)

# This causes no event because map was already created
test_map2 = client.get_map(map_name)

# This causes a DESTROYED event
test_map.destroy()

# De-register the listener
client.remove_distributed_object_listener(reg_id)

client.shutdown()
