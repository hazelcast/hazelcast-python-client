import hazelcast
import logging

if __name__ == '__main__':
    # Configure logging
    logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
    logging.getLogger().setLevel(logging.INFO)

    # Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
    hz = hazelcast.HazelcastClient()
    # Get a distributed lock called "my-distributed-lock"
    lock = hz.get_lock("my-distributed-lock")
    # Now create a lock and execute some guarded code.
    lock.lock()
    try:
        # do something here
        pass
    finally:
        lock.unlock()

    # Shutdown this Hazelcast Client
    hz.shutdown()
