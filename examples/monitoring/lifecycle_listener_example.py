import hazelcast


def on_state_change(state):
    print("State changed to", state)


client = hazelcast.HazelcastClient(lifecycle_listeners=[
    on_state_change
])

client.shutdown()
