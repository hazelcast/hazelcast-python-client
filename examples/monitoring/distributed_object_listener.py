import logging
import hazelcast

logging.basicConfig(level=logging.INFO)

client = hazelcast.HazelcastClient()


def listener(event):
    print(
        f"Name: {event.name}, "
        f"Service name: {event.service_name}, "
        f"Event type: {event.event_type}"
    )


# Register the listener
reg_id = client.add_distributed_object_listener(listener).result()

map_name = "test_map"

# This call causes a CREATED event
test_map = client.get_map(map_name)

# This causes no event because map was already created
test_map2 = client.get_map(map_name)

# This causes a DESTROYED event
test_map.destroy()

# De-register the listener
client.remove_distributed_object_listener(reg_id).result()

client.shutdown()
