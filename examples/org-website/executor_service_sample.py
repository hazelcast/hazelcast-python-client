import hazelcast
import logging

from hazelcast.serialization.api import Portable


class MessagePrinter(Portable):
    FACTORY_ID = 1
    CLASS_ID = 9

    def __init__(self, message=None):
        self.message = message

    def write_portable(self, writer):
        writer.write_utf("message", self.message)

    def read_portable(self, reader):
        self.message = reader.read_utf("message")

    def get_factory_id(self):
        return self.FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
    logging.getLogger().setLevel(logging.INFO)

    # Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
    hz = hazelcast.HazelcastClient()
    # Get the Distributed Executor Service
    ex = hz.get_executor("my-distributed-executor")
    # Get the first Hazelcast Cluster Member
    firstMember = hz.cluster.members[0]
    # Submit the MessagePrinter Runnable to the first Hazelcast Cluster Member
    ex.execute_on_member(firstMember, MessagePrinter("message to very first member of the cluster"))
    # Submit the MessagePrinter Runnable to all Hazelcast Cluster Members
    ex.execute_on_all_members(MessagePrinter("message to all members in the cluster"))
    # Submit the MessagePrinter Runnable to the Hazelcast Cluster Member owning the key called "key"
    ex.execute_on_key_owner("key", MessagePrinter("message to the member that owns the following key"))
    # Shutdown this Hazelcast Client
    hz.shutdown()
