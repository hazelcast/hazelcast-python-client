import hazelcast

client = hazelcast.HazelcastClient()

my_map = client.get_map("my-map")

# Fill the map
my_map.put("1", "Tokyo")
my_map.put("2", "Paris")
my_map.put("3", "Istanbul")

print("Entry with key 3:", my_map.get("3").result())

print("Map size:", my_map.size().result())

# Print the map
print("\nIterating over the map: \n")

entries = my_map.entry_set().result()
for key, value in entries:
    print("%s -> %s" % (key, value))

client.shutdown()
