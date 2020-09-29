import hazelcast

# Start the Hazelcast Client and connect to an already running
# Hazelcast Cluster on 127.0.0.1
# Note: CP Subsystem has to be enabled on the cluster
hz = hazelcast.HazelcastClient()
# Get the AtomicLong counter from Cluster
counter = hz.cp_subsystem.get_atomic_long("counter").blocking()
# Add and get the counter
value = counter.add_and_get(3)
print("Counter value is", value)
# Shutdown this Hazelcast client
hz.shutdown()

