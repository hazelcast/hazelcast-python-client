import logging
import hazelcast
import time

logging.basicConfig(level=logging.INFO)

client = hazelcast.HazelcastClient()

listener_map = client.get_map("listener_map").blocking()


def entry_added(event):
    print(f"Entry added with key: {event.key}, value: {event.value}")


def entry_removed(event):
    print(f"Entry removed with key: {event.key}")


def entry_updated(event):
    print(
        f"Entry updated with key: {event.key}, "
        f"old value: {event.old_value}, "
        f"new value: {event.value}"
    )


listener_map.add_entry_listener(
    include_value=True,
    added_func=entry_added,
    removed_func=entry_removed,
    updated_func=entry_updated,
)

listener_map.put("key", "value")
listener_map.put("key", "new value")
listener_map.remove("key")

time.sleep(3)

client.shutdown()
