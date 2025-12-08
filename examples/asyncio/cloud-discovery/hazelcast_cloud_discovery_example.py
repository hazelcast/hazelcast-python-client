import asyncio

from hazelcast.asyncio import HazelcastClient


async def amain():
    client = await HazelcastClient.create_and_start(
        # Set up cluster name for authentication
        cluster_name="asyncio",
        # Set the token of your cloud cluster
        cloud_discovery_token="wE1w1USF6zOnaLVjLZwbZHxEoZJhw43yyViTbe6UBTvz4tZniA",
        ssl_enabled=True,
        ssl_cafile="/path/to/ca.pem",
        ssl_certfile="/path/to/cert.pem",
        ssl_keyfile="/path/to/key.pem",
        ssl_password="05dd4498c3f",
    )
    my_map = await client.get_map("map-on-the-cloud")
    await my_map.put("key", "value")

    value = await my_map.get("key")
    print(value)

    await client.shutdown()


if __name__ == "__main__":
    asyncio.run(amain())
