import os
import unittest

import pytest

import hazelcast.errors
from tests.base import SingleMemberTestCase
from tests.util import (
    random_string,
    compare_client_version,
    skip_if_server_version_older_than,
    skip_if_client_version_older_than,
)

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
        skip_if_server_version_older_than(self, self.client, "5.5")
        name = random_string()
        self.client.create_vector_collection_config(name, [IndexConfig("vector", Metric.COSINE, 3)])
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

    def test_clear(self):
        doc = self.vector_collection.get("k1")
        self.assertIsNone(doc)
        doc = Document("v1", self.vec1([0.1, 0.2, 0.3]))
        self.vector_collection.set("k1", doc)
        self.vector_collection.clear()
        doc = self.vector_collection.get("k1")
        self.assertIsNone(doc)

    def test_optimize(self):
        doc = Document("v1", self.vec1([0.1, 0.2, 0.3]))
        self.vector_collection.set("k1", doc)
        # it is hard to observe results of optimize, so just test that the invocation works
        self.vector_collection.optimize()

    def test_optimize_with_name(self):
        doc = Document("v1", self.vec1([0.1, 0.2, 0.3]))
        self.vector_collection.set("k1", doc)
        # it is hard to observe results of optimize, so just test that the invocation works
        self.vector_collection.optimize("vector")

    def test_search_near_vector_include_all(self):
        target_doc = self.doc1("v1", [0.3, 0.4, 0.5])
        self.vector_collection.put_all(
            {
                "k1": self.doc1("v1", [0.1, 0.2, 0.3]),
                "k2": self.doc1("v1", [0.2, 0.3, 0.4]),
                "k3": target_doc,
            }
        )
        result = self.vector_collection.search_near_vector(
            self.vec1([0.2, 0.2, 0.3]), limit=1, include_vectors=True, include_value=True
        )
        self.assertEqual(1, len(result))
        self.assert_document_equal(target_doc, result[0])
        self.assertAlmostEqual(0.9973459243774414, result[0].score)

    def test_search_near_vector_include_none(self):
        target_doc = self.doc1("v1", [0.3, 0.4, 0.5])
        self.vector_collection.put_all(
            {
                "k1": self.doc1("v1", [0.1, 0.2, 0.3]),
                "k2": self.doc1("v1", [0.2, 0.3, 0.4]),
                "k3": target_doc,
            }
        )
        result = self.vector_collection.search_near_vector(
            self.vec1([0.2, 0.2, 0.3]), limit=1, include_vectors=False, include_value=False
        )
        self.assertEqual(1, len(result))
        result1 = result[0]
        self.assertAlmostEqual(0.9973459243774414, result1.score)
        self.assertIsNone(result1.value)
        self.assertIsNone(result1.vectors)

    def test_search_near_vector_hint(self):
        # not empty collection is needed for search to do something
        doc = Document("v1", self.vec1([0.1, 0.2, 0.3]))
        self.vector_collection.set("k1", doc)

        # trigger validation error to check if hint was sent
        with self.assertRaises(hazelcast.errors.IllegalArgumentError):
            self.vector_collection.search_near_vector(
                self.vec1([0.2, 0.2, 0.3]),
                limit=1,
                include_vectors=False,
                include_value=False,
                hints={"partitionLimit": "-1"},
            )

    def test_size(self):
        self.assertEqual(self.vector_collection.size(), 0)
        doc = Document("v1", Vector("vector", Type.DENSE, [0.1, 0.2, 0.3]))
        self.vector_collection.put("k1", doc)
        self.assertEqual(self.vector_collection.size(), 1)
        self.vector_collection.clear()
        self.assertEqual(self.vector_collection.size(), 0)

    def test_backup_count_valid_values_pass(self):
        skip_if_client_version_older_than(self, "5.6.0")
        name = random_string()
        self.client.create_vector_collection_config(
            name, [IndexConfig("vector", Metric.COSINE, 3)], backup_count=2, async_backup_count=2
        )
        self.client.get_vector_collection(name).blocking()

    def test_backup_count_max_value_pass(self):
        skip_if_client_version_older_than(self, "5.6.0")
        name = random_string()
        self.client.create_vector_collection_config(
            name, [IndexConfig("vector", Metric.COSINE, 3)], backup_count=6
        )
        self.client.get_vector_collection(name).blocking()

    def test_backup_count_min_value_pass(self):
        skip_if_client_version_older_than(self, "5.6.0")
        name = random_string()
        self.client.create_vector_collection_config(
            name, [IndexConfig("vector", Metric.COSINE, 3)], backup_count=0
        )
        self.client.get_vector_collection(name).blocking()

    def test_backup_count_more_than_max_value_fail(self):
        skip_if_server_version_older_than(self, self.client, "5.6.0")
        skip_if_client_version_older_than(self, "5.6.0")
        name = random_string()
        # check that the parameter is used by ensuring that it is validated on server side
        # there is no simple way to check number of backups
        with self.assertRaises(hazelcast.errors.IllegalArgumentError):
            self.client.create_vector_collection_config(
                name,
                [IndexConfig("vector", Metric.COSINE, 3)],
                backup_count=7,
                async_backup_count=0,
            )

    def test_backup_count_less_than_min_value_fail(self):
        skip_if_server_version_older_than(self, self.client, "5.6.0")
        skip_if_client_version_older_than(self, "5.6.0")
        name = random_string()
        with self.assertRaises(hazelcast.errors.IllegalArgumentError):
            self.client.create_vector_collection_config(
                name, [IndexConfig("vector", Metric.COSINE, 3)], backup_count=-1
            )

    def test_async_backup_count_max_value_pass(self):
        skip_if_client_version_older_than(self, "5.6.0")
        name = random_string()
        self.client.create_vector_collection_config(
            name,
            [IndexConfig("vector", Metric.COSINE, 3)],
            backup_count=0,
            async_backup_count=6,
        )
        self.client.get_vector_collection(name).blocking()

    def test_async_backup_count_min_value_pass(self):
        skip_if_client_version_older_than(self, "5.6.0")
        name = random_string()
        self.client.create_vector_collection_config(
            name, [IndexConfig("vector", Metric.COSINE, 3)], async_backup_count=0
        )
        self.client.get_vector_collection(name).blocking()

    def test_async_backup_count_more_than_max_value_fail(self):
        skip_if_server_version_older_than(self, self.client, "5.6.0")
        skip_if_client_version_older_than(self, "5.6.0")
        name = random_string()
        # check that the parameter is used by ensuring that it is validated on server side
        # there is no simple way to check number of backups
        with self.assertRaises(hazelcast.errors.IllegalArgumentError):
            self.client.create_vector_collection_config(
                name,
                [IndexConfig("vector", Metric.COSINE, 3)],
                backup_count=0,
                async_backup_count=7,
            )

    def test_async_backup_count_less_than_min_value_fail(self):
        skip_if_server_version_older_than(self, self.client, "5.6.0")
        skip_if_client_version_older_than(self, "5.6.0")
        name = random_string()
        with self.assertRaises(hazelcast.errors.IllegalArgumentError):
            self.client.create_vector_collection_config(
                name,
                [IndexConfig("vector", Metric.COSINE, 3)],
                async_backup_count=-1,
            )

    def test_sync_and_async_backup_count_more_than_max_value_fail(self):
        skip_if_server_version_older_than(self, self.client, "5.6.0")
        skip_if_client_version_older_than(self, "5.6.0")
        name = random_string()
        with self.assertRaises(hazelcast.errors.IllegalArgumentError):
            self.client.create_vector_collection_config(
                name,
                [IndexConfig("vector", Metric.COSINE, 3)],
                backup_count=4,
                async_backup_count=3,
            )

    def test_merge_policy_can_be_sent(self):
        skip_if_client_version_older_than(self, "5.6.0")
        name = random_string()
        self.client.create_vector_collection_config(
            name,
            [IndexConfig("vector", Metric.COSINE, 3)],
            merge_policy="DiscardMergePolicy",
            merge_batch_size=1000,
        )
        # validation happens when the collection proxy is created
        self.client.get_vector_collection(name)

    def test_wrong_merge_policy_fails(self):
        skip_if_client_version_older_than(self, "5.6.0")
        skip_if_server_version_older_than(self, self.client, "5.6.0")
        name = random_string()
        with self.assertRaises(hazelcast.errors.InvalidConfigurationError):
            self.client.create_vector_collection_config(
                name, [IndexConfig("vector", Metric.COSINE, 3)], merge_policy="non-existent"
            )
            # validation happens when the collection proxy is created
            self.client.get_vector_collection(name)

    def test_split_brain_name_can_be_sent(self):
        skip_if_client_version_older_than(self, "5.6.0")
        name = random_string()
        self.client.create_vector_collection_config(
            name,
            [IndexConfig("vector", Metric.COSINE, 3)],
            # wrong name will be ignored
            split_brain_protection_name="non-existent",
        )
        col = self.client.get_vector_collection(name)
        doc = Document("v1", Vector("vector", Type.DENSE, [0.1, 0.2, 0.3]))
        col.set("k1", doc)

    def assert_document_equal(self, doc1, doc2) -> None:
        self.assertEqual(doc1.value, doc2.value)
        self.assertEqual(len(doc1.vectors), len(doc2.vectors))
        # currently there's a bug on the server-side about vector names.
        # if there's a single vector, its name is not returned
        # see: https://hazelcast.atlassian.net/browse/HZAI-67
        # working around that for now
        skip_check_name = len(doc1.vectors) == 1
        for i in range(len(doc1.vectors)):
            self.assert_vector_equal(doc1.vectors[i], doc2.vectors[i], skip_check_name)

    def assert_vector_equal(self, vec1, vec2, skip_check_name=False):
        if not skip_check_name:
            self.assertEqual(vec1.name, vec2.name)
        self.assertEqual(vec1.type, vec2.type)
        self.assertEqual(len(vec1.vector), len(vec2.vector))
        for i in range(len(vec1.vector)):
            self.assertAlmostEqual(vec1.vector[i], vec2.vector[i])

    @classmethod
    def vec1(cls, elems):
        return Vector("vector", Type.DENSE, elems)

    @classmethod
    def doc1(cls, value, vector_elems):
        return Document(value, cls.vec1(vector_elems))
