import hazelcast
import logging


def entry_added(event):
    print("Entry added with key: {}, value: {}".format(event.key, event.value))


def entry_removed(event):
    print("Entry removed with key: {}".format(event.key))


def entry_updated(event):
    print("Entry updated with key: {}, old value: {}, new value: {}".format(event.key,
                                                                            event.old_value,
                                                                            event.value))


if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    client = hazelcast.HazelcastClient()

    my_map = client.get_map("listener-map").blocking()

    my_map.add_entry_listener(True, added_func=entry_added, removed_func=entry_removed, updated_func=entry_updated)

    my_map.put("key", "value")
    my_map.put("key", "new value")
    my_map.remove("key")

    client.shutdown()
