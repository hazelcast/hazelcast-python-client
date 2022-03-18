import logging
import hazelcast

logging.basicConfig(level=logging.INFO)


def on_state_change(state):
    print(f"State changed to: {state}")


client = hazelcast.HazelcastClient(
    lifecycle_listeners=[on_state_change],
)

client.shutdown()
