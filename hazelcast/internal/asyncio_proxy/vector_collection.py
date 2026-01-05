import asyncio
import copy
import typing
import uuid
from typing import Any, Dict, List, Optional, Tuple

from hazelcast.protocol.codec import (
    vector_collection_set_codec,
    vector_collection_get_codec,
    vector_collection_search_near_vector_codec,
    vector_collection_delete_codec,
    vector_collection_put_codec,
    vector_collection_put_if_absent_codec,
    vector_collection_remove_codec,
    vector_collection_put_all_codec,
    vector_collection_clear_codec,
    vector_collection_optimize_codec,
    vector_collection_size_codec,
)
from hazelcast.internal.asyncio_proxy.base import Proxy
from hazelcast.serialization.compact import SchemaNotReplicatedError
from hazelcast.serialization.data import Data
from hazelcast.types import KeyType, ValueType
from hazelcast.util import check_not_none
from hazelcast.vector import (
    Document,
    SearchResult,
    Vector,
    VectorType,
    VectorSearchOptions,
)


class VectorCollection(Proxy, typing.Generic[KeyType, ValueType]):
    """VectorCollection contains documents with vectors.

    Concurrent, distributed, observable and searchable vector collection.

    The configuration of the vector collection must exist before it can be used.

    Example:

        >>> await client.create_vector_collection_config("my_vc", [
        >>>    IndexConfig(name="default-vector", metric=Metric.COSINE, dimension=2)
        >>> ]
        >>> my_vc = await client.get_vector_collection("my_vc")
        >>> await my_vc.set("key1", Vector("default-vector", Type.DENSE, [0.1, 0.2])

    Warning:
        Asyncio client vector collection proxy is not thread-safe, do not access it from other threads.

    Warning:
        Asyncio client is BETA.
        Its public API may change until General Availability release.

    """

    def __init__(self, service_name, name, context):
        super(VectorCollection, self).__init__(service_name, name, context)

    async def get(self, key: Any) -> Document | None:
        """Returns the Document for the specified key, or ``None`` if this VectorCollection
        does not contain this key.

        Warning:
            This method returns a clone of the original Document. Modifying the
            returned Document does not change the actual Document in the VectorCollection.
            Put the modified Document back to make changes visible to all nodes.

        >>> doc = await my_vc.get(key)
        >>> doc.value.update_some_property()
        >>> await my_vc.set(key, doc)

        Warning:
            This method uses ``__hash__`` and ``__eq__`` methods of binary form
            of the key, not the actual implementations of ``__hash__`` and
            ``__eq__`` defined in the key's class.

        Args:
            key: The specified key.

        Returns:
            The Document for the specified key or ``None`` if there was no
            mapping for key.
        """
        check_not_none(key, "key can't be None")
        return await self._get_internal(key)

    async def set(self, key: Any, document: Document) -> None:
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
        return await self._set_internal(key, document)

    async def put(self, key: Any, document: Document) -> Document | None:
        """Associates the specified Document with the specified key in this VectorCollection.

        If the VectorCollection previously contained a mapping for the key, the old Document is
        replaced by the specified Document. If the previous value is not needed, using
        the ``set`` method is more efficient.

        Warning:
            This method returns a clone of the previous Document, not the original
            (identically equal) Document previously put into the VectorCollection.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` methods of binary form
            of the key, not the actual implementations of ``__hash__`` and
            ``__eq__`` defined in the key's class.

        Args:
            key: Key of the entry.
            document: Document of the entry.

        Returns:
            Previous Document associated with the key or ``None`` if there was no
            mapping for the key.
        """
        check_not_none(key, "key can't be None")
        check_not_none(document, "document can't be None")
        check_not_none(document.value, "document value can't be None")
        return await self._put_internal(key, document)

    async def put_all(self, map: Dict[Any, Document]) -> None:
        """Copies all the mappings from the specified dictionary to this VectorCollection.

        No atomicity guarantees are given. In the case of a failure, some
        key-document tuples may get written, while others are not.

        Args:
            map: Dictionary which includes mappings to be stored in this VectorCollection.
        """
        check_not_none(map, "map can't be None")
        if not map:
            return None
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
                return await self._send_schema_and_retry(e, self.put_all, map)

            partition_id = partition_service.get_partition_id(entry[0])
            partition_map.setdefault(partition_id, []).append(entry)

        async with asyncio.TaskGroup() as tg:  # type: ignore[attr-defined]
            for partition_id, entry_list in partition_map.items():
                request = vector_collection_put_all_codec.encode_request(self.name, entry_list)
                tg.create_task(self._ainvoke_on_partition(request, partition_id))

        return None

    async def put_if_absent(self, key: Any, document: Document) -> Document | None:
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
            Old Document for the given key or ``None`` if there is not one.
        """
        check_not_none(key, "key can't be None")
        check_not_none(document, "document can't be None")
        check_not_none(document.value, "document value can't be None")
        return await self._put_if_absent_internal(key, document)

    async def search_near_vector(
        self,
        vector: Vector,
        *,
        include_value: bool = False,
        include_vectors: bool = False,
        limit: int = 10,
        hints: Dict[str, str] = None
    ) -> List[SearchResult]:
        """Returns the Documents closest to the given vector.

        The search is performed using the distance metric set when
        creating the vector index.

        Args:
            vector: The vector to be used as the reference.
                It must have the same dimension as specified when creating the vector index.
            include_value: Return value attached to the Document.
            include_vectors: Return vectors attached to the Document.
            limit: Limit the maximum number of Documents returned.
                If not set, ``10`` is used as the default limit.

        Returns:
            List of search results.
        """
        check_not_none(vector, "vector can't be None")
        if limit <= 0:
            raise AssertionError("limit must be positive")
        return await self._search_near_vector_internal(
            vector,
            include_value=include_value,
            include_vectors=include_vectors,
            limit=limit,
            hints=hints,
        )

    async def remove(self, key: Any) -> Document | None:
        """Removes the mapping for a key from this VectorCollection if it is present
        (optional operation).

        The VectorCollection will not contain a mapping for the specified key once the call
        returns.

        Warning:
            This method uses ``__hash__`` and ``__eq__`` methods of binary form
            of the key, not the actual implementations of ``__hash__`` and
            ``__eq__`` defined in the key's class.

        Args:
            key: Key of the mapping to be deleted.

        Returns:
            The Document associated with key, or ``None`` if there was
            no mapping for key.
        """
        check_not_none(key, "key can't be None")
        return await self._remove_internal(key)

    async def delete(self, key: Any) -> None:
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
        return await self._delete_internal(key)

    async def optimize(self, index_name: str = None) -> None:
        """Optimize index by fully removing nodes marked for deletion, trimming neighbor sets
        to the advertised degree, and updating the entry node as necessary.

        Warning:
            This operation can take a long time to execute and consume a lot of server resources.

        Args:
            index_name: Name of the index to optimize. If not specified, the only index defined
                for the collection will be used. Must be specified if the collection has more than
                one index.
        """
        request = vector_collection_optimize_codec.encode_request(
            self.name, index_name, uuid.uuid4()
        )
        return await self._invoke(request)

    async def clear(self) -> None:
        """Clears the VectorCollection."""
        request = vector_collection_clear_codec.encode_request(self.name)
        return await self._invoke(request)

    async def size(self) -> int:
        """Returns the number of Documents in this VectorCollection.

        Returns:
            Number of Documents in this VectorCollection.
        """
        request = vector_collection_size_codec.encode_request(self.name)
        return await self._invoke(request, vector_collection_size_codec.decode_response)

    async def _set_internal(self, key: Any, document: Document) -> None:
        try:
            key_data = self._to_data(key)
            value_data = self._to_data(document.value)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.set, key, document)
        document = copy.copy(document)
        document.value = value_data
        request = vector_collection_set_codec.encode_request(
            self.name,
            key_data,
            document,
        )
        return await self._invoke_on_key(request, key_data)

    async def _get_internal(self, key: Any) -> Any:
        def handler(message):
            doc = vector_collection_get_codec.decode_response(message)
            return self._transform_document(doc)

        try:
            key_data = self._to_data(key)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.get, key)
        request = vector_collection_get_codec.encode_request(
            self.name,
            key_data,
        )
        return await self._invoke_on_key(request, key_data, response_handler=handler)

    def _search_near_vector_internal(
        self,
        vector: Vector,
        *,
        include_value: bool = False,
        include_vectors: bool = False,
        limit: int = 10,
        hints: Dict[str, str] = None
    ) -> asyncio.Future[List[SearchResult]]:
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
            include_value=include_value,
            include_vectors=include_vectors,
            limit=limit,
            hints=hints or {},
        )
        request = vector_collection_search_near_vector_codec.encode_request(
            self.name,
            [vector],
            options,
        )
        return self._invoke(request, response_handler=handler)

    async def _delete_internal(self, key: Any) -> None:
        key_data = self._to_data(key)
        request = vector_collection_delete_codec.encode_request(self.name, key_data)
        return await self._invoke_on_key(request, key_data)

    async def _remove_internal(self, key: Any) -> Document | None:
        def handler(message):
            doc = vector_collection_remove_codec.decode_response(message)
            return self._transform_document(doc)

        key_data = self._to_data(key)
        request = vector_collection_remove_codec.encode_request(self.name, key_data)
        return await self._invoke_on_key(request, key_data, response_handler=handler)

    async def _put_internal(self, key: Any, document: Document) -> Document | None:
        def handler(message):
            doc = vector_collection_put_codec.decode_response(message)
            return self._transform_document(doc)

        try:
            key_data = self._to_data(key)
            value_data = self._to_data(document.value)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.set, key, document)
        document = copy.copy(document)
        document.value = value_data
        request = vector_collection_put_codec.encode_request(
            self.name,
            key_data,
            document,
        )
        return await self._invoke_on_key(request, key_data, response_handler=handler)

    async def _put_if_absent_internal(self, key: Any, document: Document) -> Document | None:
        def handler(message):
            doc = vector_collection_put_if_absent_codec.decode_response(message)
            return self._transform_document(doc)

        try:
            key_data = self._to_data(key)
            value_data = self._to_data(document.value)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.set, key, document)
        document.value = value_data
        request = vector_collection_put_if_absent_codec.encode_request(
            self.name,
            key_data,
            document,
        )
        return await self._invoke_on_key(request, key_data, response_handler=handler)

    def _transform_document(self, doc: Optional[Document]) -> Optional[Document]:
        if doc is not None:
            if doc.value is not None:
                doc.value = self._to_object(doc.value)
            for vec in doc.vectors:
                vec.type = VectorType(vec.type)
        return doc


async def create_vector_collection_proxy(service_name, name, context):
    return VectorCollection(service_name, name, context)
