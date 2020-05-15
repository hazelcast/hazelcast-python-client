import hazelcast


def entry_added(event):
    print("Entry added with key: {}, value: {}".format(event.key, event.value))


if __name__ == '__main__':
    client = hazelcast.HazelcastClient()

    my_map = client.get_map("listener_map").blocking()

    my_map.add_entry_listener(key='key1', include_value=True, added_func=entry_added)

    my_map.put("key", "value")
    my_map.put("key1", "new_value")
    my_map.remove("key")
    my_map.remove("key1")

    client.shutdown()
