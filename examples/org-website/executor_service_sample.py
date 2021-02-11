import hazelcast

from hazelcast.serialization.api import Portable


class MessagePrinter(Portable):
    FACTORY_ID = 1
    CLASS_ID = 9

    def __init__(self, message=None):
        self.message = message

    def write_portable(self, writer):
        writer.write_string("message", self.message)

    def read_portable(self, reader):
        self.message = reader.read_string("message")

    def get_factory_id(self):
        return self.FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID


# Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
client = hazelcast.HazelcastClient()
# Get the Distributed Executor Service
ex = client.get_executor("my-distributed-executor")
# Get the an Hazelcast Cluster Member
member = client.cluster_service.get_members()[0]
# Submit the MessagePrinter Runnable to the first Hazelcast Cluster Member
ex.execute_on_member(member, MessagePrinter("message to very first member of the cluster"))
# Submit the MessagePrinter Runnable to all Hazelcast Cluster Members
ex.execute_on_all_members(MessagePrinter("message to all members in the cluster"))
# Submit the MessagePrinter Runnable to the Hazelcast Cluster Member owning the key called "key"
ex.execute_on_key_owner("key", MessagePrinter("message to the member that owns the following key"))
# Shutdown this Hazelcast Client
client.shutdown()
