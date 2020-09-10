import hazelcast

# Connect
client = hazelcast.HazelcastClient(
    cluster_members=[
        "127.0.0.1:5701"
    ]
)

# Get a map that is stored on the server side. We can access it from the client
greetings_map = client.get_map("greetings-map").blocking()

# Map is empty on the first run. It will be non-empty if Hazelcast has data on this map
print("Size before:", greetings_map.size())

# Write data to map. If there is a data with the same key already, it will be overwritten
greetings_map.put("English", "hello world")
greetings_map.put("Spanish", "hola mundo")
greetings_map.put("Italian", "ciao mondo")
greetings_map.put("German", "hallo welt")
greetings_map.put("French", "bonjour monde")

# 5 data is added to the map. There should be at least 5 data on the server side
print("Size after:", greetings_map.size())

# Shutdown the client
client.shutdown()
