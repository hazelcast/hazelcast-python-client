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
    vector_collection_clear_codec,
    vector_collection_optimize_codec,
    vector_collection_size_codec,
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
    def __init__(self, service_name, name, context):
        super(VectorCollection, self).__init__(service_name, name, context)

    def blocking(self) -> "BlockingVectorCollection":
        return BlockingVectorCollection(self)

    def get(self, key: Any) -> Future[Optional[Document]]:
        check_not_none(key, "key can't be None")
        return self._get_internal(key)

    def set(self, key: Any, document: Document) -> Future[None]:
        check_not_none(key, "key can't be None")
        check_not_none(document, "document can't be None")
        check_not_none(document.value, "document value can't be None")
        return self._set_internal(key, document)

    def put(self, key: Any, document: Document) -> Future[Optional[Document]]:
        check_not_none(key, "key can't be None")
        check_not_none(document, "document can't be None")
        check_not_none(document.value, "document value can't be None")
        return self._put_internal(key, document)

    def put_all(self, map: Dict[Any, Document]) -> Future[None]:
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
        limit: int
    ) -> Future[List[SearchResult]]:
        check_not_none(vector, "vector can't be None")
        return self._search_near_vector_internal(
            vector,
            include_value=include_value,
            include_vectors=include_vectors,
            limit=limit,
        )

    def remove(self, key: Any) -> Future[Optional[Document]]:
        check_not_none(key, "key can't be None")
        return self._remove_internal(key)

    def delete(self, key: Any) -> Future[None]:
        check_not_none(key, "key can't be None")
        return self._delete_internal(key)

    def optimize(self, index_name: str = None) -> Future[None]:
        request = vector_collection_optimize_codec.encode_request(self.name, index_name)
        return self._invoke(request)

    def clear(self) -> Future[None]:
        request = vector_collection_clear_codec.encode_request(self.name)
        return self._invoke(request)

    def size(self) -> Future[int]:
        request = vector_collection_size_codec.encode_request(self.name)
        return self._invoke(request, vector_collection_size_codec.decode_response)

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
        limit: int
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
            include_value=include_value,
            include_vectors=include_vectors,
            limit=limit,
        )
        request = vector_collection_search_near_vector_codec.encode_request(
            self.name,
            [vector],
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
        limit: int
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

    def clear(self) -> None:
        return self._wrapped.clear().result()

    def optimize(self, index_name: str = None) -> None:
        return self._wrapped.optimize(index_name).result()

    def size(self) -> int:
        return self._wrapped.size().result()

    def destroy(self) -> bool:
        return self._wrapped.destroy()
