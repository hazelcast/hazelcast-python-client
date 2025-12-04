from uuid import uuid4

import asyncio


async def fill_map(map, size=10, key_prefix="key", value_prefix="val"):
    entries = dict()
    for i in range(size):
        entries[key_prefix + str(i)] = value_prefix + str(i)
    await map.put_all(entries)
    return entries


async def open_connection_to_address(client, uuid):
    key = generate_key_owned_by_instance(client, uuid)
    m = await client.get_map(str(uuid4()))
    await m.put(key, 0)
    await m.destroy()


def generate_key_owned_by_instance(client, uuid):
    while True:
        key = str(uuid4())
        partition_id = client.partition_service.get_partition_id(key)
        owner = str(client.partition_service.get_partition_owner(partition_id))
        if owner == uuid:
            return key


async def wait_for_partition_table(client):
    m = await client.get_map(str(uuid4()))
    while not client.partition_service.get_partition_owner(0):
        await m.put(str(uuid4()), 0)
        await asyncio.sleep(0.1)
