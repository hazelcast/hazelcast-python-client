import hazelcast

# Start the Hazelcast Client and connect to an already running
# Hazelcast Cluster on 127.0.0.1
client = hazelcast.HazelcastClient()
# Get the Distributed Lock from CP Subsystem
lock = client.cp_subsystem.get_lock("my-distributed-lock").blocking()
# Now acquire the lock and execute some guarded code
fence = lock.lock()
print("Fence token:", fence)
try:
    # do something here
    pass
finally:
    lock.unlock()

# Shutdown this Hazelcast Client
client.shutdown()
