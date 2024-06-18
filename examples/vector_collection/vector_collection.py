import logging

from hazelcast.vector import Type, Vector, Metric, IndexConfig

logging.basicConfig(level=logging.DEBUG)

from hazelcast.client import HazelcastClient, Config
from hazelcast.proxy.vector_collection import Document


def main():
    # Connect to the cluster
    cfg = Config()
    cfg.cluster_members = ["localhost:5701"]
    client = HazelcastClient(cfg)

    vc_name = "my-vector-collection"

    # Create the VectorCollection
    indexes = [
        IndexConfig(name="default-vector", metric=Metric.COSINE, dimension=2),
    ]
    client.create_vector_collection_config(vc_name, indexes=indexes)

    # Use the VectorCollection
    vc = client.get_vector_collection(vc_name).blocking()
    doc1 = Document(
        "value1",
        [
            Vector("default-vector", Type.DENSE, [0.1, 0.5]),
        ],
    )

    # Add the Document
    key = "key-1"
    vc.set(key, doc1)

    # Add another Document
    doc2 = Document(
        "value2",
        [
            Vector("default-vector", Type.DENSE, [0.5, 0.7]),
        ],
    )

    # Add the Document
    key = "key-2"
    vc.set(key, doc2)

    # Optimize collection
    vc.optimize()

    # Search for a vector
    results = vc.search_near_vector(
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

    print("size:", vc.size())

    # Delete all entries
    vc.clear()
    print("cleared collection")
    print("size:", vc.size())

    # Search for a vector
    results = vc.search_near_vector(
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
    main()
