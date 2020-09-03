import hazelcast
import time


def member_added(member):
    print("Member added: {}".format(member))


def member_removed(member):
    print("Member removed: {}".format(member))


client = hazelcast.HazelcastClient()
client.cluster_service.add_listener(member_added, member_removed, True)

# Add/Remove member now to see the listeners in action
time.sleep(100)
client.shutdown()
