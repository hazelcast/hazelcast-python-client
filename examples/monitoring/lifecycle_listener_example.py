import hazelcast


def on_state_change(state):
    print("State changed to {}".format(state))


if __name__ == "__main__":
    config = hazelcast.ClientConfig()
    config.add_lifecycle_listener(on_state_change)

    client = hazelcast.HazelcastClient(config)

    client.shutdown()
