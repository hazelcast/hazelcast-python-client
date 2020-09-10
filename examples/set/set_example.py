import hazelcast

client = hazelcast.HazelcastClient()

my_set = client.get_set("set")

my_set.add("Item1")
my_set.add("Item1")
my_set.add("Item2")

found = my_set.contains("Item2").result()
print("Set contains Item2:", found)

items = my_set.get_all().result()
print("Size of set:", len(items))

print("\nAll Items:")
for item in items:
    print(item)

client.shutdown()
