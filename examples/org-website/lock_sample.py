import logging
import hazelcast

logging.basicConfig(level=logging.INFO)

# Start the Hazelcast Client and connect to an already running
# Hazelcast Cluster on 127.0.0.1
client = hazelcast.HazelcastClient()

# Get the Distributed Lock from CP Subsystem
distributed_lock = client.cp_subsystem.get_lock("distributed-lock").blocking()

# Now acquire the lock and execute some guarded code
fence = distributed_lock.lock()
print(f"Fence token: {fence}")

try:
    # do something here
    pass
finally:
    distributed_lock.unlock()

# Shutdown this Hazelcast Client
client.shutdown()
