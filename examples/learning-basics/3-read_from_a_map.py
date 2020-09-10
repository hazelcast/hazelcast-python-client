import hazelcast

# Connect
client = hazelcast.HazelcastClient(
    cluster_members=[
        "127.0.0.1:5701"
    ]
)

# We can access maps on the server from the client. Let's access the greetings map that we created already
greetings_map = client.get_map("greetings-map").blocking()

# Get the entry set of the map
entry_set = greetings_map.entry_set()

# Print key-value pairs
for key, value in entry_set:
    print("%s -> %s" % (key, value))

# Shutdown the client
client.shutdown()
