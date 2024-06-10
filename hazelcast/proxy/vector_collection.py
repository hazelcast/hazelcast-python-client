import copy
from typing import Any, Dict, List, Optional, Tuple

from hazelcast.future import Future, ImmediateFuture, combine_futures
from hazelcast.protocol.codec import (
    vector_collection_set_codec,
    vector_collection_get_codec,
    vector_collection_search_near_vector_codec,
    vector_collection_delete_codec,
    vector_collection_put_codec,
    vector_collection_put_if_absent_codec,
    vector_collection_remove_codec,
    vector_collection_put_all_codec,
)
from hazelcast.proxy import Proxy
from hazelcast.serialization.compact import SchemaNotReplicatedError
from hazelcast.serialization.data import Data
from hazelcast.util import check_not_none
from hazelcast.vector import (
    Document,
    SearchResult,
    Vector,
    VectorType,
    VectorSearchOptions,
)


class VectorCollection(Proxy["BlockingVectorCollection"]):
    """VectorCollection contains documents with vectors.

    Concurrent, distributed, observable and searchable vector collection.
    The vector collection can work both async(non-blocking) or sync(blocking).
    Blocking calls return the value of the call and block the execution until return value is calculated.
    However, async calls return ``Future`` and do not block execution.
    Result of the ``Future`` can be used whenever ready.
    A ``Future``'s result can be obtained with blocking the execution by calling ``future.result()``.

    The configuration of the vector collection must exist before it can be used.

    Example:
        #
        client.create_vector_collection_config("my_vc", [
            IndexConfig(name="default-vector", metric=Metric.COSINE, dimension=2)
        ]
        my_vc = client.get_vector_collection("my_vc").blocking()
        my_vc.set("key1", Vector("default-vector", Type.DENSE, [0.1, 0.2])
    """

    def __init__(self, service_name, name, context):
        super(VectorCollection, self).__init__(service_name, name, context)

    def blocking(self) -> "BlockingVectorCollection":
        """Returns a blocking variant of VectorCollection"""
        return BlockingVectorCollection(self)

    def get(self, key: Any) -> Future[Optional[Document]]:
        """Returns the value for the specified key, or ``None`` if this VectorCollection
        does not contain this key.

        Warning:
            This method returns a clone of original value, modifying the
            returned value does not change the actual value in the VectorCollection. One
            should put modified value back to make changes visible to all nodes.

                >>> value = my_vc.get(key)
                >>> value.update_some_property()
                >>> my_vc.set(key,value)

        Warning:
            This method uses ``__hash__`` and ``__eq__`` methods of binary form
            of the key, not the actual implementations of ``__hash__`` and
            ``__eq__`` defined in key's class.

        Args:
            key: The specified key.

        Returns:
            The value for the specified key.
        """
        check_not_none(key, "key can't be None")
        return self._get_internal(key)

    def set(self, key: Any, document: Document) -> Future[None]:
        """Sets a document for the given key in the VectorCollection.

        Similar to the put operation except that set doesn't return the old
        document, which is more efficient.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` methods of binary form
            of the key, not the actual implementations of ``__hash__`` and
            ``__eq__`` defined in key's class.

        Args:
            key: Key of the entry.
            document: Document of the entry.
        """
        check_not_none(key, "key can't be None")
        check_not_none(document, "document can't be None")
        check_not_none(document.value, "document value can't be None")
        return self._set_internal(key, document)

    def put(self, key: Any, document: Document) -> Future[Optional[Document]]:
        """Associates the specified Document with the specified key in this VectorCollection.

        If the VectorCollection previously contained a mapping for the key, the old Document is
        replaced by the specified Document.

        Warning:
            This method returns a clone of the previous Document, not the original
            (identically equal) Document previously put into the VectorCollection.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` methods of binary form
            of the key, not the actual implementations of ``__hash__`` and
            ``__eq__`` defined in key's class.

        Args:
            key: Key of the entry.
            document: Document of the entry.

        Returns:
            Previous Document associated with key or ``None`` if there was no
            mapping for key.
        """
        check_not_none(key, "key can't be None")
        check_not_none(document, "document can't be None")
        check_not_none(document.value, "document value can't be None")
        return self._put_internal(key, document)

    def put_all(self, map: Dict[Any, Document]) -> Future[None]:
        """Copies all the mappings from the specified dictionary to this VectorCollection.

        No atomicity guarantees are given. In the case of a failure, some
        key-document tuples may get written, while others are not.

        Args:
            map: Dictionary which includes mappings to be stored in this VectorCollection.
        """
        check_not_none(map, "map can't be None")
        if not map:
            return ImmediateFuture(None)
        partition_service = self._context.partition_service
        partition_map: Dict[int, List[Tuple[Data, Document]]] = {}

        for key, doc in map.items():
            check_not_none(key, "key can't be None")
            check_not_none(doc, "value can't be None")
            doc = copy.copy(doc)
            try:
                entry = (self._to_data(key), doc)
                doc.value = self._to_data(doc.value)
            except SchemaNotReplicatedError as e:
                return self._send_schema_and_retry(e, self.put_all, map)

            partition_id = partition_service.get_partition_id(entry[0])
            partition_map.setdefault(partition_id, []).append(entry)

        futures = []
        for partition_id, entry_list in partition_map.items():
            request = vector_collection_put_all_codec.encode_request(self.name, entry_list)
            future = self._invoke_on_partition(request, partition_id)
            futures.append(future)

        return combine_futures(futures)

    def put_if_absent(self, key: Any, document: Document) -> Future[Optional[Document]]:
        """Associates the specified key with the given Document if it is not
        already associated.

        Warning:
            This method returns a clone of the previous Document, not the original
            (identically equal) Document previously put into the VectorCollection.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` methods of binary form
            of the key, not the actual implementations of ``__hash__`` and
            ``__eq__`` defined in key's class.

        Args:
            key: Key of the entry.
            document: Document of the entry.

        Returns:
            Old Document for the given key.
        """
        check_not_none(key, "key can't be None")
        check_not_none(document, "document can't be None")
        check_not_none(document.value, "document value can't be None")
        return self._put_if_absent_internal(key, document)

    def search_near_vector(
        self,
        vector: Vector,
        *,
        include_value: bool = False,
        include_vectors: bool = False,
        limit: int = -1
    ) -> Future[List[SearchResult]]:
        """Returns the Documents closest to the given vector.

        The search is performed using distance metric set when
        creating the vector index.

        Args:
            vector: The vector to be used as the reference.
                It must have the same dimension as specified when creating the vector index.
            include_value: Return value attached to the Document.
            include_vectors: Return vectors attached to the Document.
            limit: Limit the maximum number of Documents returned.
                If not set, the server default is used.

        Returns:
            List of search results.
        """
        check_not_none(vector, "vector can't be None")
        return self._search_near_vector_internal(
            vector,
            include_value=include_value,
            include_vectors=include_vectors,
            limit=limit,
        )

    def remove(self, key: Any) -> Future[Optional[Document]]:
        """Removes the mapping for a key from this VectorCollection if it is present.

        The VectorCollection will not contain a mapping for the specified key once the call
        returns.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` methods of binary form
            of the key, not the actual implementations of ``__hash__`` and
            ``__eq__`` defined in key's class.

        Args:
            key: Key of the mapping to be deleted.

        Returns:
            The Document associated with key, or ``None`` if there was
            no mapping for key.
        """
        check_not_none(key, "key can't be None")
        return self._remove_internal(key)

    def delete(self, key: Any) -> Future[None]:
        """Removes the mapping for a key from this VectorCollection if it is present
        (optional operation).

        Unlike remove(object), this operation does not return the removed
        Document, which avoids the serialization cost of the returned Document.
        If the removed Document will not be used, a delete operation is preferred
        over a remove operation for better performance.

        The VectorCollection will not contain a mapping for the specified key once the call
        returns.

        Args:
            key: Key of the mapping to be deleted.
        """
        check_not_none(key, "key can't be None")
        return self._delete_internal(key)

    def _set_internal(self, key: Any, document: Document) -> Future[None]:
        try:
            key_data = self._to_data(key)
            value_data = self._to_data(document.value)
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.set, key, document)
        document = copy.copy(document)
        document.value = value_data
        request = vector_collection_set_codec.encode_request(
            self.name,
            key_data,
            document,
        )
        return self._invoke_on_key(request, key_data)

    def _get_internal(self, key: Any):
        def handler(message):
            doc = vector_collection_get_codec.decode_response(message)
            return self._transform_document(doc)

        try:
            key_data = self._to_data(key)
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.get, key)
        request = vector_collection_get_codec.encode_request(
            self.name,
            key_data,
        )
        return self._invoke_on_key(request, key_data, response_handler=handler)

    def _search_near_vector_internal(
        self,
        vector: Vector,
        *,
        include_value: bool = False,
        include_vectors: bool = False,
        limit: int = -1
    ) -> Future[List[SearchResult]]:
        def handler(message):
            results: List[
                SearchResult
            ] = vector_collection_search_near_vector_codec.decode_response(message)
            for result in results:
                if result.key is not None:
                    result.key = self._to_object(result.key)
                if result.value is not None:
                    result.value = self._to_object(result.value)
                if result.vectors:
                    for vec in result.vectors:
                        vec.type = VectorType(vec.type)
            return results

        options = VectorSearchOptions(
            vectors=[vector],
            include_value=include_value,
            include_vectors=include_vectors,
            limit=limit,
        )
        request = vector_collection_search_near_vector_codec.encode_request(
            self.name,
            options,
        )
        return self._invoke(request, response_handler=handler)

    def _delete_internal(self, key: Any) -> Future[None]:
        key_data = self._to_data(key)
        request = vector_collection_delete_codec.encode_request(self.name, key_data)
        return self._invoke_on_key(request, key_data)

    def _remove_internal(self, key: Any) -> Future[Optional[Document]]:
        def handler(message):
            doc = vector_collection_remove_codec.decode_response(message)
            return self._transform_document(doc)

        key_data = self._to_data(key)
        request = vector_collection_remove_codec.encode_request(self.name, key_data)
        return self._invoke_on_key(request, key_data, response_handler=handler)

    def _put_internal(self, key: Any, document: Document) -> Future[Optional[Document]]:
        def handler(message):
            doc = vector_collection_put_codec.decode_response(message)
            return self._transform_document(doc)

        try:
            key_data = self._to_data(key)
            value_data = self._to_data(document.value)
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.set, key, document)
        document = copy.copy(document)
        document.value = value_data
        request = vector_collection_put_codec.encode_request(
            self.name,
            key_data,
            document,
        )
        return self._invoke_on_key(request, key_data, response_handler=handler)

    def _put_if_absent_internal(self, key: Any, document: Document) -> Future[Optional[Document]]:
        def handler(message):
            doc = vector_collection_put_if_absent_codec.decode_response(message)
            return self._transform_document(doc)

        try:
            key_data = self._to_data(key)
            value_data = self._to_data(document.value)
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.set, key, document)
        document.value = value_data
        request = vector_collection_put_if_absent_codec.encode_request(
            self.name,
            key_data,
            document,
        )
        return self._invoke_on_key(request, key_data, response_handler=handler)

    def _transform_document(self, doc: Optional[Document]) -> Optional[Document]:
        if doc is not None:
            if doc.value is not None:
                doc.value = self._to_object(doc.value)
            for vec in doc.vectors:
                vec.type = VectorType(vec.type)
        return doc


class BlockingVectorCollection:

    __slots__ = ("_wrapped",)

    def __init__(self, wrapped: VectorCollection):
        self._wrapped = wrapped

    def get(self, key: Any) -> Optional[Document]:
        return self._wrapped.get(key).result()

    def set(self, key: Any, document: Document) -> None:
        return self._wrapped.set(key, document).result()

    def search_near_vector(
        self,
        vector: Vector,
        *,
        include_value: bool = False,
        include_vectors: bool = False,
        limit: int = -1
    ) -> List[SearchResult]:
        future = self._wrapped.search_near_vector(
            vector,
            include_value=include_value,
            include_vectors=include_vectors,
            limit=limit,
        )
        return future.result()

    def delete(self, key: Any) -> None:
        return self._wrapped.delete(key).result()

    def remove(self, key: Any) -> Optional[Document]:
        return self._wrapped.remove(key).result()

    def put(self, key: Any, document: Document) -> Optional[Document]:
        return self._wrapped.put(key, document).result()

    def put_all(self, map: Dict[Any, Document]) -> None:
        return self._wrapped.put_all(map).result()

    def put_if_absent(self, key: Any, document: Document) -> Optional[Document]:
        return self._wrapped.put_if_absent(key, document).result()

    def destroy(self) -> bool:
        return self._wrapped.destroy()
