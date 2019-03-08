import hazelcast

if __name__ == "__main__":
    # Start the Hazelcast Client and connect to an already running Hazelcast Cluster on 127.0.0.1
    hz = hazelcast.HazelcastClient()
    # Get an Atomic Counter, we'll call it "counter"
    counter = hz.get_atomic_long("counter").blocking()
    # Add and Get the "counter"
    counter.add_and_get(3)
    # value is 3
    # Display the "counter" value
    print("counter: {}".format(counter.get()))
    # Shutdown this Hazelcast Client
    hz.shutdown()
