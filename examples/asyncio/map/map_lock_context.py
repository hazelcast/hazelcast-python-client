import asyncio

from hazelcast.asyncio import HazelcastClient


async def amain():
    async def f():
        # prevent a data race on key
        async with map.lock_context(key):
            value = await map.get(key)
            await map.set(key, value + 1)

    client = await HazelcastClient.create_and_start()
    map = await client.get_map("lock-example")
    key = "k1"
    await map.set(key, 0)

    # create 100 concurrent tasks
    async with asyncio.TaskGroup() as tg:
        for i in range(100):
            tg.create_task(f())

    # retrieve the final value
    value = await map.get(key)
    print(f"value: {value}")


if __name__ == "__main__":
    asyncio.run(amain())
