import asyncio

from hazelcast.asyncio import HazelcastClient
from hazelcast import Int8


async def amain():
    client = await HazelcastClient.create_and_start()
    map = await client.get_map("number_test")
    await map.set("i8", Int8(10))
    value_i8 = await map.get("i8")
    assert type(value_i8) == int


asyncio.run(amain())
