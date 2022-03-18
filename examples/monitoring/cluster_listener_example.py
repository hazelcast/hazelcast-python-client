import logging
import hazelcast
import time

logging.basicConfig(level=logging.INFO)

client = hazelcast.HazelcastClient()


def member_added(member):
    print(f"Added member: {member}")


def member_removed(member):
    print(f"Removed member: {member}")


client.cluster_service.add_listener(
    member_added=member_added,
    member_removed=member_removed,
    fire_for_existing=True,
)

# Add/Remove member now to see the listeners in action
time.sleep(100)
client.shutdown()
