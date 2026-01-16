import asyncio
import logging

from hazelcast.vector import Type, Vector, Metric, IndexConfig

logging.basicConfig(level=logging.DEBUG)

from hazelcast.asyncio import HazelcastClient
from hazelcast.config import Config
from hazelcast.proxy.vector_collection import Document


async def amain():
    # Connect to the cluster
    cfg = Config()
    cfg.cluster_members = ["localhost:5701"]
    client = await HazelcastClient.create_and_start(cfg)

    vc_name = "my-vector-collection"

    # Create the VectorCollection
    indexes = [
        IndexConfig(name="default-vector", metric=Metric.COSINE, dimension=2),
    ]
    await client.create_vector_collection_config(vc_name, indexes=indexes)

    # Use the VectorCollection
    vc = await client.get_vector_collection(vc_name)
    doc1 = Document(
        "value1",
        [
            Vector("default-vector", Type.DENSE, [0.1, 0.5]),
        ],
    )

    # Add the Document
    key = "key-1"
    await vc.set(key, doc1)

    # Add another Document
    doc2 = Document(
        "value2",
        [
            Vector("default-vector", Type.DENSE, [0.5, 0.7]),
        ],
    )

    # Add the Document
    key = "key-2"
    await vc.set(key, doc2)

    # Optimize collection
    await vc.optimize()

    # Search for a vector
    results = await vc.search_near_vector(
        Vector("default-vector", Type.DENSE, [0.2, 0.3]),
        limit=2,
        include_value=True,
        include_vectors=True,
    )
    for i, result in enumerate(results):
        print(
            f"{i+1}.",
            "Key:",
            result.key,
            "Value:",
            result.value,
            "Score:",
            result.score,
            "Vector:",
            result.vectors,
        )

    print("size:", await vc.size())

    # Delete all entries
    await vc.clear()
    print("cleared collection")
    print("size:", await vc.size())

    # Search for a vector
    results = await vc.search_near_vector(
        Vector("default-vector", Type.DENSE, [0.2, 0.3]),
        limit=2,
        include_value=True,
        include_vectors=True,
    )
    for i, result in enumerate(results):
        print(
            f"{i+1}.",
            "Key:",
            result.key,
            "Value:",
            result.value,
            "Score:",
            result.score,
            "Vector:",
            result.vectors,
        )


if __name__ == "__main__":
    asyncio.run(amain())
