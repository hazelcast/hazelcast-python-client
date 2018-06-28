import hazelcast
import logging
from hazelcast import six

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
    logging.getLogger().setLevel(logging.INFO)
    logger = logging.getLogger("main")

    # Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
    hz = hazelcast.HazelcastClient()
    # Get an Atomic Counter, we'll call it "counter"
    counter = hz.get_atomic_long("counter")
    # Add and Get the "counter"
    counter.add_and_get(3).result()  # value is 3
    # Display the "counter" value
    six.print_("counter: ", counter.get().result())
    # Shutdown this Hazelcast Client
    hz.shutdown()
