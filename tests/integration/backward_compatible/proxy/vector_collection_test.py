import os
import unittest

import pytest

from tests.base import SingleMemberTestCase
from tests.util import random_string, compare_client_version

try:
    from hazelcast.vector import IndexConfig, Metric, Document, Vector, Type
except ImportError:
    # backward compatibility
    pass


@unittest.skipIf(
    compare_client_version("5.5") < 0, "Tests the features added in 5.5 version of the client"
)
@pytest.mark.enterprise
class VectorCollectionTest(SingleMemberTestCase):
    @classmethod
    def configure_cluster(cls):
        path = os.path.abspath(__file__)
        dir_path = os.path.dirname(path)
        with open(os.path.join(dir_path, "hazelcast.xml")) as f:
            return f.read()

    @classmethod
    def configure_client(cls, config):
        config["cluster_name"] = cls.cluster.id
        return config

    def setUp(self):
        name = random_string()
        self.client.create_vector_collection(name, [IndexConfig("vector", Metric.COSINE, 3)])
        self.vector_collection = self.client.get_vector_collection(name).blocking()

    def tearDown(self):
        self.vector_collection.destroy()

    def test_set(self):
        doc = Document("v1", Vector("vector", Type.DENSE, [0.1, 0.2, 0.3]))
        self.vector_collection.set("k1", doc)

    def test_get(self):
        doc = self.vector_collection.get("k1")
        self.assertIsNone(doc)
        doc = Document("v1", Vector("vector", Type.DENSE, [0.1, 0.2, 0.3]))
        self.vector_collection.set("k1", doc)
        got_doc = self.vector_collection.get("k1")
        self.assert_document_equal(got_doc, doc)

    def test_put(self):
        doc = Document("v1", Vector("vector", Type.DENSE, [0.1, 0.2, 0.3]))
        doc_old = self.vector_collection.put("k1", doc)
        self.assertIsNone(doc_old)
        doc2 = Document("v1", Vector("vector", Type.DENSE, [0.4, 0.5, 0.6]))
        doc_old = self.vector_collection.put("k1", doc2)
        self.assert_document_equal(doc_old, doc)

    def test_delete(self):
        doc = self.vector_collection.get("k1")
        self.assertIsNone(doc)
        doc = Document("v1", Vector("vector", Type.DENSE, [0.1, 0.2, 0.3]))
        self.vector_collection.set("k1", doc)
        self.vector_collection.delete("k1")
        doc = self.vector_collection.get("k1")
        self.assertIsNone(doc)

    def test_remove(self):
        doc = self.vector_collection.get("k1")
        self.assertIsNone(doc)
        doc = Document("v1", Vector("vector", Type.DENSE, [0.1, 0.2, 0.3]))
        self.vector_collection.set("k1", doc)
        doc2 = self.vector_collection.remove("k1")
        self.assert_document_equal(doc, doc2)

    def test_put_all(self):
        doc1 = self.doc1("v1", [0.1, 0.2, 0.3])
        doc2 = self.doc1("v1", [0.2, 0.3, 0.4])
        self.vector_collection.put_all(
            {
                "k1": doc1,
                "k2": doc2,
            }
        )
        k1 = self.vector_collection.get("k1")
        self.assert_document_equal(k1, doc1)
        k2 = self.vector_collection.get("k2")
        self.assert_document_equal(k2, doc2)

    def test_search_near_vector(self):
        self.vector_collection.put_all(
            {
                "k1": self.doc1("v1", [0.1, 0.2, 0.3]),
                "k2": self.doc1("v1", [0.2, 0.3, 0.4]),
                "k3": self.doc1("v1", [0.3, 0.4, 0.5]),
            }
        )
        result = self.vector_collection.search_near_vector(
            self.vec1([0.2, 0.2, 0.3]), limit=1, include_vectors=True, include_value=True
        )
        print("result:", result)

    def assert_document_equal(self, doc1: Document, doc2: Document) -> None:
        self.assertEqual(doc1.value, doc2.value)
        self.assertEqual(len(doc1.vectors), len(doc2.vectors))
        # currently there's a bug on the server-side about vector names.
        # if there's a single vector, its name is not returned
        # see: https://hazelcast.atlassian.net/browse/HZAI-67
        # working around that for now
        skip_check_name = len(doc1.vectors) == 1
        for i in range(len(doc1.vectors)):
            self.assert_vector_equal(doc1.vectors[i], doc2.vectors[i], skip_check_name)

    def assert_vector_equal(self, vec1: Vector, vec2: Vector, skip_check_name=False):
        if not skip_check_name:
            self.assertEqual(vec1.name, vec2.name)
        self.assertEqual(vec1.type, vec2.type)
        self.assertEqual(len(vec1.vector), len(vec2.vector))
        for i in range(len(vec1.vector)):
            self.assertAlmostEqual(vec1.vector[i], vec2.vector[i])

    @classmethod
    def vec1(cls, elems) -> Vector:
        return Vector("vector", Type.DENSE, elems)

    @classmethod
    def doc1(cls, value, vector_elems) -> Document:
        return Document(value, cls.vec1(vector_elems))
