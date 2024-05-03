import logging

from hazelcast import vector
from hazelcast.core import HazelcastJsonValue
from hazelcast.vector import Type, Vector

logging.basicConfig(level=logging.DEBUG)

from hazelcast.client import HazelcastClient, Config
from hazelcast.proxy.vector_collection import Document


def main():
    # Connect to the cluster
    cfg = Config()
    cfg.cluster_members = ["localhost:5701"]
    client = HazelcastClient(cfg)

    vc_name = "my-vc6"

    # Create the VectorCollection
    indexes = [
        vector.IndexConfig(name="vector-name1", metric=vector.Metric.COSINE, dimension=2),
        vector.IndexConfig(name="vector-name2", metric=vector.Metric.COSINE, dimension=2),
    ]
    client.create_vector_collection(vc_name, indexes=indexes)

    # Use the VecotorCollection
    vc = client.get_vector_collection(vc_name).blocking()
    doc = Document(
        "some-value",
        [
            Vector("vector-name1", Type.DENSE, [1.0, 0.5]),
            Vector("vector-name2", Type.DENSE, [1.0, 0.5]),
        ],
    )

    # Add the Document
    key = "some-key"
    vc.set(key, doc)

    # Retrieve the Document
    doc = vc.get(key)
    print("Document:", doc)

    # Search for a vector
    results = vc.search_near_vector(
        [0.9, 0.4],
        include_value=True,
        include_vectors=True,
        target_vector="vector-name1",
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

    doc = vc.put(
        key,
        Document(
            "bar",
            [
                Vector("vector-name1", Type.DENSE, [0.5, -0.5]),
                Vector("vector-name2", Type.DENSE, [0.6, -0.6]),
            ],
        ),
    )
    print("Document:", doc)

    vc.delete(key)


if __name__ == "__main__":
    main()
