import time

import hazelcast


def entry_added(event):
    print("Entry added with key: %s, value: %s" % (event.key, event.value))


def entry_removed(event):
    print("Entry removed with key:", event.key)


def entry_updated(event):
    print("Entry updated with key: %s, old value: %s, new value: %s" % (event.key, event.old_value, event.value))


client = hazelcast.HazelcastClient()

my_map = client.get_map("listener-map").blocking()

my_map.add_entry_listener(True, added_func=entry_added, removed_func=entry_removed, updated_func=entry_updated)

my_map.put("key", "value")
my_map.put("key", "new value")
my_map.remove("key")

time.sleep(3)

client.shutdown()
