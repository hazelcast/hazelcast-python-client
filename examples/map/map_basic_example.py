import logging
import hazelcast

logging.basicConfig(level=logging.INFO)

client = hazelcast.HazelcastClient()

capitals = client.get_map("capitals").blocking()

# Fill the map
capitals.put("1", "Tokyo")
capitals.put("2", "Paris")
capitals.put("3", "Ankara")

print(f"Entry with key 3: {capitals.get('3')}")

print(f"Map size: {capitals.size()}")

# Print the map
for key, value in capitals.entry_set():
    print(f"{key} -> {value}")

client.shutdown()
