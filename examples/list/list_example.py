import logging
import hazelcast

logging.basicConfig(level=logging.INFO)

client = hazelcast.HazelcastClient()

cities = client.get_list("cities").blocking()

cities.add("Tokyo")
cities.add("Paris")
cities.add("London")
cities.add("New York")
cities.add("Istanbul")

list_size = cities.size()
print(f"List size: {list_size}")

first_element = cities.get(0)
print(f"First element: {first_element}")

contains_istanbul = cities.contains("Istanbul")
print(f"Contains Istanbul: {contains_istanbul}")

sublist = cities.sub_list(3, 5)
print(f"Sublist: {sublist}")

cities.remove("Tokyo")

final_size = cities.size()
print(f"Final size: {final_size}")

client.shutdown()
