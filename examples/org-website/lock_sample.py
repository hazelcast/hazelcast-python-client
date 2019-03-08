import hazelcast

if __name__ == "__main__":
    # Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
    hz = hazelcast.HazelcastClient()
    # Get a distributed lock called "my-distributed-lock"
    lock = hz.get_lock("my-distributed-lock").blocking()
    # Now create a lock and execute some guarded code.
    lock.lock()
    try:
        # do something here
        pass
    finally:
        lock.unlock()

    # Shutdown this Hazelcast Client
    hz.shutdown()
