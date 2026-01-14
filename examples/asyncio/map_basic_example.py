import asyncio

from hazelcast.asyncio import HazelcastClient


async def amain():
    client = await HazelcastClient.create_and_start()
    my_map = await client.get_map("my-map")

    # Fill the map
    await my_map.put("1", "Tokyo")
    await my_map.put("2", "Paris")
    await my_map.put("3", "Istanbul")

    entry = await my_map.get("3")
    print("Entry with key 3:", entry)

    map_size = await my_map.size()
    print("Map size:", map_size)

    # Print the map
    print("\nIterating over the map: \n")

    entries = await my_map.entry_set()
    for key, value in entries:
        print("%s -> %s" % (key, value))

    await client.shutdown()


if __name__ == "__main__":
    asyncio.run(amain())
