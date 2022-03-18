import logging
import hazelcast

logging.basicConfig(level=logging.INFO)

client = hazelcast.HazelcastClient()

names = client.get_set("names").blocking()

names.add("John")
names.add("Jake")
names.add("Jade")

found = names.contains("Jake")
print(f"Set contains Jake: {found}")

items = names.get_all()
print(f"Size of set: {len(items)}")

for item in items:
    print(item)

client.shutdown()
