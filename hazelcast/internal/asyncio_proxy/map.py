import asyncio
import itertools
import typing

from hazelcast.aggregator import Aggregator
from hazelcast.config import IndexUtil, IndexType, IndexConfig
from hazelcast.core import SimpleEntryView
from hazelcast.errors import InvalidConfigurationError
from hazelcast.projection import Projection
from hazelcast.protocol import PagingPredicateHolder
from hazelcast.protocol.codec import (
    map_add_entry_listener_codec,
    map_add_entry_listener_to_key_codec,
    map_add_entry_listener_with_predicate_codec,
    map_add_entry_listener_to_key_with_predicate_codec,
    map_clear_codec,
    map_contains_key_codec,
    map_contains_value_codec,
    map_delete_codec,
    map_entry_set_codec,
    map_entries_with_predicate_codec,
    map_evict_codec,
    map_evict_all_codec,
    map_flush_codec,
    map_get_codec,
    map_get_all_codec,
    map_get_entry_view_codec,
    map_is_empty_codec,
    map_key_set_codec,
    map_key_set_with_predicate_codec,
    map_load_all_codec,
    map_load_given_keys_codec,
    map_put_codec,
    map_put_all_codec,
    map_put_if_absent_codec,
    map_put_transient_codec,
    map_size_codec,
    map_remove_codec,
    map_remove_if_same_codec,
    map_remove_entry_listener_codec,
    map_replace_codec,
    map_replace_if_same_codec,
    map_set_codec,
    map_try_put_codec,
    map_try_remove_codec,
    map_values_codec,
    map_values_with_predicate_codec,
    map_add_interceptor_codec,
    map_aggregate_codec,
    map_aggregate_with_predicate_codec,
    map_project_codec,
    map_project_with_predicate_codec,
    map_execute_on_all_keys_codec,
    map_execute_on_key_codec,
    map_execute_on_keys_codec,
    map_execute_with_predicate_codec,
    map_add_index_codec,
    map_set_ttl_codec,
    map_entries_with_paging_predicate_codec,
    map_key_set_with_paging_predicate_codec,
    map_values_with_paging_predicate_codec,
    map_put_with_max_idle_codec,
    map_put_if_absent_with_max_idle_codec,
    map_put_transient_with_max_idle_codec,
    map_set_with_max_idle_codec,
    map_remove_interceptor_codec,
    map_remove_all_codec,
)
from hazelcast.internal.asyncio_proxy.base import (
    Proxy,
    EntryEvent,
    EntryEventType,
    get_entry_listener_flags,
)
from hazelcast.predicate import Predicate, _PagingPredicate
from hazelcast.serialization.data import Data
from hazelcast.types import AggregatorResultType, KeyType, ValueType, ProjectionType
from hazelcast.serialization.compact import SchemaNotReplicatedError
from hazelcast.util import (
    check_not_none,
    thread_id,
    to_millis,
    IterationType,
    deserialize_entry_list_in_place,
    deserialize_list_in_place,
)


EntryEventCallable = typing.Callable[[EntryEvent[KeyType, ValueType]], None]


class Map(Proxy, typing.Generic[KeyType, ValueType]):
    def __init__(self, service_name, name, context):
        super(Map, self).__init__(service_name, name, context)
        self._reference_id_generator = context.lock_reference_id_generator

    async def add_entry_listener(
        self,
        include_value: bool = False,
        key: KeyType = None,
        predicate: Predicate = None,
        added_func: EntryEventCallable = None,
        removed_func: EntryEventCallable = None,
        updated_func: EntryEventCallable = None,
        evicted_func: EntryEventCallable = None,
        evict_all_func: EntryEventCallable = None,
        clear_all_func: EntryEventCallable = None,
        merged_func: EntryEventCallable = None,
        expired_func: EntryEventCallable = None,
        loaded_func: EntryEventCallable = None,
    ) -> str:
        flags = get_entry_listener_flags(
            ADDED=added_func,
            REMOVED=removed_func,
            UPDATED=updated_func,
            EVICTED=evicted_func,
            EXPIRED=expired_func,
            EVICT_ALL=evict_all_func,
            CLEAR_ALL=clear_all_func,
            MERGED=merged_func,
            LOADED=loaded_func,
        )
        if key is not None and predicate is not None:
            try:
                key_data = self._to_data(key)
                predicate_data = self._to_data(predicate)
            except SchemaNotReplicatedError as e:
                return await self._send_schema_and_retry(
                    e,
                    self.add_entry_listener,
                    include_value,
                    key,
                    predicate,
                    added_func,
                    removed_func,
                    updated_func,
                    evicted_func,
                    evict_all_func,
                    clear_all_func,
                    merged_func,
                    expired_func,
                    loaded_func,
                )
            with_key_and_predicate_codec = map_add_entry_listener_to_key_with_predicate_codec
            request = with_key_and_predicate_codec.encode_request(
                self.name, key_data, predicate_data, include_value, flags, self._is_smart
            )
            response_decoder = with_key_and_predicate_codec.decode_response
            event_message_handler = with_key_and_predicate_codec.handle
        elif key is not None and predicate is None:
            try:
                key_data = self._to_data(key)
            except SchemaNotReplicatedError as e:
                return await self._send_schema_and_retry(
                    e,
                    self.add_entry_listener,
                    include_value,
                    key,
                    predicate,
                    added_func,
                    removed_func,
                    updated_func,
                    evicted_func,
                    evict_all_func,
                    clear_all_func,
                    merged_func,
                    expired_func,
                    loaded_func,
                )

            with_key_codec = map_add_entry_listener_to_key_codec
            request = with_key_codec.encode_request(
                self.name, key_data, include_value, flags, self._is_smart
            )
            response_decoder = with_key_codec.decode_response
            event_message_handler = with_key_codec.handle
        elif key is None and predicate is not None:
            try:
                predicate = self._to_data(predicate)
            except SchemaNotReplicatedError as e:
                return await self._send_schema_and_retry(
                    e,
                    self.add_entry_listener,
                    include_value,
                    key,
                    predicate,
                    added_func,
                    removed_func,
                    updated_func,
                    evicted_func,
                    evict_all_func,
                    clear_all_func,
                    merged_func,
                    expired_func,
                    loaded_func,
                )
            with_predicate_codec = map_add_entry_listener_with_predicate_codec
            request = with_predicate_codec.encode_request(
                self.name, predicate, include_value, flags, self._is_smart
            )
            response_decoder = with_predicate_codec.decode_response
            event_message_handler = with_predicate_codec.handle
        else:
            codec = map_add_entry_listener_codec
            request = codec.encode_request(self.name, include_value, flags, self._is_smart)
            response_decoder = codec.decode_response
            event_message_handler = codec.handle

        def handle_event_entry(
            key_data,
            value_data,
            old_value_data,
            merging_value_data,
            event_type,
            uuid,
            number_of_affected_entries,
        ):
            event = EntryEvent(
                self._to_object(key_data),
                self._to_object(value_data),
                self._to_object(old_value_data),
                self._to_object(merging_value_data),
                event_type,
                uuid,
                number_of_affected_entries,
            )
            if event.event_type == EntryEventType.ADDED:
                added_func(event)
            elif event.event_type == EntryEventType.REMOVED:
                removed_func(event)
            elif event.event_type == EntryEventType.UPDATED:
                updated_func(event)
            elif event.event_type == EntryEventType.EVICTED:
                evicted_func(event)
            elif event.event_type == EntryEventType.EVICT_ALL:
                evict_all_func(event)
            elif event.event_type == EntryEventType.CLEAR_ALL:
                clear_all_func(event)
            elif event.event_type == EntryEventType.MERGED:
                merged_func(event)
            elif event.event_type == EntryEventType.EXPIRED:
                expired_func(event)
            elif event.event_type == EntryEventType.LOADED:
                loaded_func(event)

        return await self._register_listener(
            request,
            lambda r: response_decoder(r),
            lambda reg_id: map_remove_entry_listener_codec.encode_request(self.name, reg_id),
            lambda m: event_message_handler(m, handle_event_entry),
        )

    async def add_index(
        self,
        attributes: typing.Sequence[str] = None,
        index_type: typing.Union[int, str] = IndexType.SORTED,
        name: str = None,
        bitmap_index_options: typing.Dict[str, typing.Any] = None,
    ) -> None:
        d = {
            "name": name,
            "type": index_type,
            "attributes": attributes,
            "bitmap_index_options": bitmap_index_options,
        }
        config = IndexConfig.from_dict(d)
        validated = IndexUtil.validate_and_normalize(self.name, config)
        request = map_add_index_codec.encode_request(self.name, validated)
        return await self._invoke(request)

    async def add_interceptor(self, interceptor: typing.Any) -> str:
        try:
            interceptor_data = self._to_data(interceptor)
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.add_interceptor, interceptor)

        request = map_add_interceptor_codec.encode_request(self.name, interceptor_data)
        return await self._invoke(request, map_add_interceptor_codec.decode_response)

    async def aggregate(
        self, aggregator: Aggregator[AggregatorResultType], predicate: Predicate = None
    ) -> AggregatorResultType:
        check_not_none(aggregator, "aggregator can't be none")
        if predicate:
            if isinstance(predicate, _PagingPredicate):
                raise AssertionError("Paging predicate is not supported.")

            try:
                aggregator_data = self._to_data(aggregator)
                predicate_data = self._to_data(predicate)
            except SchemaNotReplicatedError as e:
                return await self._send_schema_and_retry(e, self.aggregate, aggregator, predicate)

            def handler(message):
                return self._to_object(map_aggregate_with_predicate_codec.decode_response(message))

            request = map_aggregate_with_predicate_codec.encode_request(
                self.name, aggregator_data, predicate_data
            )
        else:
            try:
                aggregator_data = self._to_data(aggregator)
            except SchemaNotReplicatedError as e:
                return await self._send_schema_and_retry(e, self.aggregate, aggregator, predicate)

            def handler(message):
                return self._to_object(map_aggregate_codec.decode_response(message))

            request = map_aggregate_codec.encode_request(self.name, aggregator_data)

        return await self._invoke(request, handler)

    async def clear(self) -> None:
        request = map_clear_codec.encode_request(self.name)
        return await self._invoke(request)

    async def contains_key(self, key: KeyType) -> bool:
        check_not_none(key, "key can't be None")
        try:
            key_data = self._to_data(key)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.contains_key, key)

        return await self._contains_key_internal(key_data)

    async def contains_value(self, value: ValueType) -> bool:
        check_not_none(value, "value can't be None")
        try:
            value_data = self._to_data(value)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.contains_value, value)
        request = map_contains_value_codec.encode_request(self.name, value_data)
        return await self._invoke(request, map_contains_value_codec.decode_response)

    async def delete(self, key: KeyType) -> None:
        check_not_none(key, "key can't be None")
        try:
            key_data = self._to_data(key)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.delete, key)
        return await self._delete_internal(key_data)

    async def entry_set(
        self, predicate: Predicate = None
    ) -> typing.List[typing.Tuple[KeyType, ValueType]]:
        if predicate:
            if isinstance(predicate, _PagingPredicate):
                predicate.iteration_type = IterationType.ENTRY
                try:
                    holder = PagingPredicateHolder.of(predicate, self._to_data)
                except SchemaNotReplicatedError as e:
                    return await self._send_schema_and_retry(e, self.entry_set, predicate)

                def handler(message):
                    response = map_entries_with_paging_predicate_codec.decode_response(message)
                    predicate.anchor_list = response["anchor_data_list"].as_anchor_list(
                        self._to_object
                    )
                    entry_data_list = response["response"]
                    return deserialize_entry_list_in_place(entry_data_list, self._to_object)

                request = map_entries_with_paging_predicate_codec.encode_request(self.name, holder)
            else:
                try:
                    predicate_data = self._to_data(predicate)
                except SchemaNotReplicatedError as e:
                    return await self._send_schema_and_retry(e, self.entry_set, predicate)

                def handler(message):
                    entry_data_list = map_entries_with_predicate_codec.decode_response(message)
                    return deserialize_entry_list_in_place(entry_data_list, self._to_object)

                request = map_entries_with_predicate_codec.encode_request(self.name, predicate_data)
        else:

            def handler(message):
                entry_data_list = map_entry_set_codec.decode_response(message)
                return deserialize_entry_list_in_place(entry_data_list, self._to_object)

            request = map_entry_set_codec.encode_request(self.name)

        return await self._invoke(request, handler)

    async def evict(self, key: KeyType) -> bool:
        check_not_none(key, "key can't be None")
        try:
            key_data = self._to_data(key)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.evict, key)

        return await self._evict_internal(key_data)

    async def evict_all(self) -> None:
        request = map_evict_all_codec.encode_request(self.name)
        return await self._invoke(request)

    async def execute_on_entries(
        self, entry_processor: typing.Any, predicate: Predicate = None
    ) -> typing.List[typing.Any]:
        if predicate:
            try:
                entry_processor_data = self._to_data(entry_processor)
                predicate_data = self._to_data(predicate)
            except SchemaNotReplicatedError as e:
                return await self._send_schema_and_retry(
                    e, self.execute_on_entries, entry_processor, predicate
                )

            def handler(message):
                entry_data_list = map_execute_with_predicate_codec.decode_response(message)
                return deserialize_entry_list_in_place(entry_data_list, self._to_object)

            request = map_execute_with_predicate_codec.encode_request(
                self.name, entry_processor_data, predicate_data
            )
        else:
            try:
                entry_processor_data = self._to_data(entry_processor)
            except SchemaNotReplicatedError as e:
                return await self._send_schema_and_retry(
                    e, self.execute_on_entries, entry_processor, predicate
                )

            def handler(message):
                entry_data_list = map_execute_on_all_keys_codec.decode_response(message)
                return deserialize_entry_list_in_place(entry_data_list, self._to_object)

            request = map_execute_on_all_keys_codec.encode_request(self.name, entry_processor_data)

        return await self._invoke(request, handler)

    async def execute_on_key(self, key: KeyType, entry_processor: typing.Any) -> typing.Any:
        check_not_none(key, "key can't be None")
        try:
            key_data = self._to_data(key)
            entry_processor_data = self._to_data(entry_processor)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.execute_on_key, key, entry_processor)

        return await self._execute_on_key_internal(key_data, entry_processor_data)

    async def execute_on_keys(
        self, keys: typing.Sequence[KeyType], entry_processor: typing.Any
    ) -> typing.List[typing.Any]:
        if len(keys) == 0:
            return []
        try:
            key_list = []
            for key in keys:
                check_not_none(key, "key can't be None")
                key_list.append(self._to_data(key))

            entry_processor_data = self._to_data(entry_processor)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.execute_on_keys, keys, entry_processor)

        def handler(message):
            entry_data_list = map_execute_on_keys_codec.decode_response(message)
            return deserialize_entry_list_in_place(entry_data_list, self._to_object)

        request = map_execute_on_keys_codec.encode_request(
            self.name, entry_processor_data, key_list
        )
        return await self._invoke(request, handler)

    async def flush(self) -> None:
        request = map_flush_codec.encode_request(self.name)
        return await self._invoke(request)

    async def get(self, key: KeyType) -> typing.Optional[ValueType]:
        check_not_none(key, "key can't be None")
        try:
            key_data = self._to_data(key)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.get, key)
        return await self._get_internal(key_data)

    async def get_all(self, keys: typing.Sequence[KeyType]) -> typing.Dict[KeyType, ValueType]:
        check_not_none(keys, "keys can't be None")
        if not keys:
            return {}
        partition_service = self._context.partition_service
        partition_to_keys: typing.Dict[int, typing.Dict[KeyType, Data]] = {}
        for key in keys:
            check_not_none(key, "key can't be None")
            try:
                key_data = self._to_data(key)
            except SchemaNotReplicatedError as e:
                return await self._send_schema_and_retry(e, self.get_all, keys)
            partition_id = partition_service.get_partition_id(key_data)
            try:
                partition_to_keys[partition_id][key] = key_data
            except KeyError:
                partition_to_keys[partition_id] = {key: key_data}

        return await self._get_all_internal(partition_to_keys)

    async def get_entry_view(self, key: KeyType) -> SimpleEntryView[KeyType, ValueType]:
        check_not_none(key, "key can't be None")
        try:
            key_data = self._to_data(key)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.get_entry_view, key)

        def handler(message):
            response = map_get_entry_view_codec.decode_response(message)
            entry_view = response["response"]
            if not entry_view:
                return None
            entry_view.key = self._to_object(entry_view.key)
            entry_view.value = self._to_object(entry_view.value)
            return entry_view

        request = map_get_entry_view_codec.encode_request(self.name, key_data, thread_id())
        return await self._invoke_on_key(request, key_data, handler)

    async def is_empty(self) -> bool:
        request = map_is_empty_codec.encode_request(self.name)
        return await self._invoke(request, map_is_empty_codec.decode_response)

    async def key_set(self, predicate: Predicate = None) -> typing.List[ValueType]:
        if predicate:
            if isinstance(predicate, _PagingPredicate):
                predicate.iteration_type = IterationType.KEY

                try:
                    holder = PagingPredicateHolder.of(predicate, self._to_data)
                except SchemaNotReplicatedError as e:
                    return await self._send_schema_and_retry(e, self.key_set, predicate)

                def handler(message):
                    response = map_key_set_with_paging_predicate_codec.decode_response(message)
                    predicate.anchor_list = response["anchor_data_list"].as_anchor_list(
                        self._to_object
                    )
                    data_list = response["response"]
                    return deserialize_list_in_place(data_list, self._to_object)

                request = map_key_set_with_paging_predicate_codec.encode_request(self.name, holder)
            else:
                try:
                    predicate_data = self._to_data(predicate)
                except SchemaNotReplicatedError as e:
                    return await self._send_schema_and_retry(e, self.key_set, predicate)

                def handler(message):
                    data_list = map_key_set_with_predicate_codec.decode_response(message)
                    return deserialize_list_in_place(data_list, self._to_object)

                request = map_key_set_with_predicate_codec.encode_request(self.name, predicate_data)
        else:

            def handler(message):
                data_list = map_key_set_codec.decode_response(message)
                return deserialize_list_in_place(data_list, self._to_object)

            request = map_key_set_codec.encode_request(self.name)

        return await self._invoke(request, handler)

    async def load_all(
        self, keys: typing.Sequence[KeyType] = None, replace_existing_values: bool = True
    ) -> None:
        if keys:
            try:
                key_data_list = [self._to_data(key) for key in keys]
            except SchemaNotReplicatedError as e:
                return await self._send_schema_and_retry(
                    e, self.load_all, keys, replace_existing_values
                )

            return await self._load_all_internal(key_data_list, replace_existing_values)

        request = map_load_all_codec.encode_request(self.name, replace_existing_values)
        return await self._invoke(request)

    async def project(
        self, projection: Projection[ProjectionType], predicate: Predicate = None
    ) -> ProjectionType:
        check_not_none(projection, "Projection can't be none")
        if predicate:
            if isinstance(predicate, _PagingPredicate):
                raise AssertionError("Paging predicate is not supported.")
            try:
                projection_data = self._to_data(projection)
                predicate_data = self._to_data(predicate)
            except SchemaNotReplicatedError as e:
                return await self._send_schema_and_retry(e, self.project, projection, predicate)

            def handler(message):
                data_list = map_project_with_predicate_codec.decode_response(message)
                return deserialize_list_in_place(data_list, self._to_object)

            request = map_project_with_predicate_codec.encode_request(
                self.name, projection_data, predicate_data
            )
        else:
            try:
                projection_data = self._to_data(projection)
            except SchemaNotReplicatedError as e:
                return await self._send_schema_and_retry(e, self.project, projection, predicate)

            def handler(message):
                data_list = map_project_codec.decode_response(message)
                return deserialize_list_in_place(data_list, self._to_object)

            request = map_project_codec.encode_request(self.name, projection_data)

        return await self._invoke(request, handler)

    async def put(
        self, key: KeyType, value: ValueType, ttl: float = None, max_idle: float = None
    ) -> typing.Optional[ValueType]:
        check_not_none(key, "key can't be None")
        check_not_none(value, "value can't be None")
        try:
            key_data = self._to_data(key)
            value_data = self._to_data(value)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.put, key, value, ttl, max_idle)

        return await self._put_internal(key_data, value_data, ttl, max_idle)

    async def put_all(self, map: typing.Dict[KeyType, ValueType]) -> None:
        check_not_none(map, "map can't be None")
        if not map:
            return None
        partition_service = self._context.partition_service
        partition_map: typing.Dict[int, typing.List[typing.Tuple[Data, Data]]] = {}
        for key, value in map.items():
            check_not_none(key, "key can't be None")
            check_not_none(value, "value can't be None")
            try:
                entry = (self._to_data(key), self._to_data(value))
            except SchemaNotReplicatedError as e:
                return await self._send_schema_and_retry(e, self.put_all, map)
            partition_id = partition_service.get_partition_id(entry[0])
            try:
                partition_map[partition_id].append(entry)
            except KeyError:
                partition_map[partition_id] = [entry]

        async with asyncio.TaskGroup() as tg:  # type: ignore[attr-defined]
            for partition_id, entry_list in partition_map.items():
                request = map_put_all_codec.encode_request(
                    self.name, entry_list, False
                )  # TODO trigger map loader
                tg.create_task(self._ainvoke_on_partition(request, partition_id))
        return None

    async def put_if_absent(
        self, key: KeyType, value: ValueType, ttl: float = None, max_idle: float = None
    ) -> typing.Optional[ValueType]:
        check_not_none(key, "key can't be None")
        check_not_none(value, "value can't be None")
        try:
            key_data = self._to_data(key)
            value_data = self._to_data(value)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(
                e, self.put_if_absent, key, value, ttl, max_idle
            )

        return await self._put_if_absent_internal(key_data, value_data, ttl, max_idle)

    async def put_transient(
        self, key: KeyType, value: ValueType, ttl: float = None, max_idle: float = None
    ) -> None:
        check_not_none(key, "key can't be None")
        check_not_none(value, "value can't be None")
        try:
            key_data = self._to_data(key)
            value_data = self._to_data(value)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(
                e, self.put_transient, key, value, ttl, max_idle
            )

        return await self._put_transient_internal(key_data, value_data, ttl, max_idle)

    async def remove(self, key: KeyType) -> typing.Optional[ValueType]:
        check_not_none(key, "key can't be None")
        try:
            key_data = self._to_data(key)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.remove, key)

        return await self._remove_internal(key_data)

    async def remove_all(self, predicate: Predicate) -> None:
        check_not_none(predicate, "predicate can't be None")
        try:
            predicate_data = self._to_data(predicate)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.remove_all, predicate)

        return await self._remove_all_internal(predicate_data)

    async def remove_if_same(self, key: KeyType, value: ValueType) -> bool:
        check_not_none(key, "key can't be None")
        check_not_none(value, "value can't be None")
        try:
            key_data = self._to_data(key)
            value_data = self._to_data(value)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.remove_if_same, key, value)
        return await self._remove_if_same_internal_(key_data, value_data)

    async def remove_entry_listener(self, registration_id: str) -> bool:
        return await self._deregister_listener(registration_id)

    async def remove_interceptor(self, registration_id: str) -> bool:
        check_not_none(registration_id, "Interceptor registration id should not be None")
        request = map_remove_interceptor_codec.encode_request(self.name, registration_id)
        return await self._invoke(request, map_remove_interceptor_codec.decode_response)

    async def replace(self, key: KeyType, value: ValueType) -> typing.Optional[ValueType]:
        check_not_none(key, "key can't be None")
        check_not_none(value, "value can't be None")
        try:
            key_data = self._to_data(key)
            value_data = self._to_data(value)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.replace, key, value)
        return await self._replace_internal(key_data, value_data)

    async def replace_if_same(
        self, key: ValueType, old_value: ValueType, new_value: ValueType
    ) -> bool:
        check_not_none(key, "key can't be None")
        check_not_none(old_value, "old_value can't be None")
        check_not_none(new_value, "new_value can't be None")
        try:
            key_data = self._to_data(key)
            old_value_data = self._to_data(old_value)
            new_value_data = self._to_data(new_value)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(
                e, self.replace_if_same, key, old_value, new_value
            )

        return await self._replace_if_same_internal(key_data, old_value_data, new_value_data)

    async def set(
        self, key: KeyType, value: ValueType, ttl: float = None, max_idle: float = None
    ) -> None:
        check_not_none(key, "key can't be None")
        check_not_none(value, "value can't be None")
        try:
            key_data = self._to_data(key)
            value_data = self._to_data(value)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.set, key, value, ttl, max_idle)
        return await self._set_internal(key_data, value_data, ttl, max_idle)

    async def set_ttl(self, key: KeyType, ttl: float) -> None:
        check_not_none(key, "key can't be None")
        check_not_none(ttl, "ttl can't be None")
        try:
            key_data = self._to_data(key)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.set_ttl, key, ttl)
        return await self._set_ttl_internal(key_data, ttl)

    async def size(self) -> int:
        request = map_size_codec.encode_request(self.name)
        return await self._invoke(request, map_size_codec.decode_response)

    async def try_put(self, key: KeyType, value: ValueType, timeout: float = 0) -> bool:
        check_not_none(key, "key can't be None")
        check_not_none(value, "value can't be None")
        try:
            key_data = self._to_data(key)
            value_data = self._to_data(value)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.try_put, key, value, timeout)
        return await self._try_put_internal(key_data, value_data, timeout)

    async def try_remove(self, key: KeyType, timeout: float = 0) -> bool:
        check_not_none(key, "key can't be None")
        try:
            key_data = self._to_data(key)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.try_remove, key, timeout)
        return await self._try_remove_internal(key_data, timeout)

    async def values(self, predicate: Predicate = None) -> typing.List[ValueType]:
        if predicate:
            if isinstance(predicate, _PagingPredicate):
                predicate.iteration_type = IterationType.VALUE

                try:
                    holder = PagingPredicateHolder.of(predicate, self._to_data)
                except SchemaNotReplicatedError as e:
                    return await self._send_schema_and_retry(e, self.values, predicate)

                def handler(message):
                    response = map_values_with_paging_predicate_codec.decode_response(message)
                    predicate.anchor_list = response["anchor_data_list"].as_anchor_list(
                        self._to_object
                    )
                    data_list = response["response"]
                    return deserialize_list_in_place(data_list, self._to_object)

                request = map_values_with_paging_predicate_codec.encode_request(self.name, holder)
            else:
                try:
                    predicate_data = self._to_data(predicate)
                except SchemaNotReplicatedError as e:
                    return await self._send_schema_and_retry(e, self.values, predicate)

                def handler(message):
                    data_list = map_values_with_predicate_codec.decode_response(message)
                    return deserialize_list_in_place(data_list, self._to_object)

                request = map_values_with_predicate_codec.encode_request(self.name, predicate_data)
        else:

            def handler(message):
                data_list = map_values_codec.decode_response(message)
                return deserialize_list_in_place(data_list, self._to_object)

            request = map_values_codec.encode_request(self.name)

        return await self._invoke(request, handler)

    def _contains_key_internal(self, key_data):
        request = map_contains_key_codec.encode_request(self.name, key_data, thread_id())
        return self._invoke_on_key(request, key_data, map_contains_key_codec.decode_response)

    def _get_internal(self, key_data):
        def handler(message):
            return self._to_object(map_get_codec.decode_response(message))

        request = map_get_codec.encode_request(self.name, key_data, thread_id())
        return self._invoke_on_key(request, key_data, handler)

    async def _get_all_internal(self, partition_to_keys, tasks=None):
        def handler(message):
            entry_data_list = map_get_all_codec.decode_response(message)
            return deserialize_entry_list_in_place(entry_data_list, self._to_object)

        tasks = tasks or []
        async with asyncio.TaskGroup() as tg:
            for partition_id, key_dict in partition_to_keys.items():
                request = map_get_all_codec.encode_request(self.name, key_dict.values())
                task = tg.create_task(self._ainvoke_on_partition(request, partition_id, handler))
                tasks.append(task)
        kvs = itertools.chain.from_iterable(task.result() for task in tasks)
        return dict(kvs)

    def _remove_internal(self, key_data):
        def handler(message):
            return self._to_object(map_remove_codec.decode_response(message))

        request = map_remove_codec.encode_request(self.name, key_data, thread_id())
        return self._invoke_on_key(request, key_data, handler)

    def _remove_all_internal(self, predicate_data):
        request = map_remove_all_codec.encode_request(self.name, predicate_data)
        return self._invoke(request)

    def _remove_if_same_internal_(self, key_data, value_data):
        request = map_remove_if_same_codec.encode_request(
            self.name, key_data, value_data, thread_id()
        )
        return self._invoke_on_key(
            request, key_data, response_handler=map_remove_if_same_codec.decode_response
        )

    def _delete_internal(self, key_data):
        request = map_delete_codec.encode_request(self.name, key_data, thread_id())
        return self._invoke_on_key(request, key_data)

    def _put_internal(self, key_data, value_data, ttl, max_idle):
        def handler(message):
            return self._to_object(map_put_codec.decode_response(message))

        if max_idle is not None:
            request = map_put_with_max_idle_codec.encode_request(
                self.name, key_data, value_data, thread_id(), to_millis(ttl), to_millis(max_idle)
            )
        else:
            request = map_put_codec.encode_request(
                self.name, key_data, value_data, thread_id(), to_millis(ttl)
            )
        return self._invoke_on_key(request, key_data, handler)

    def _set_internal(self, key_data, value_data, ttl, max_idle):
        if max_idle is not None:
            request = map_set_with_max_idle_codec.encode_request(
                self.name, key_data, value_data, thread_id(), to_millis(ttl), to_millis(max_idle)
            )
        else:
            request = map_set_codec.encode_request(
                self.name, key_data, value_data, thread_id(), to_millis(ttl)
            )
        return self._invoke_on_key(request, key_data)

    def _set_ttl_internal(self, key_data, ttl):
        request = map_set_ttl_codec.encode_request(self.name, key_data, to_millis(ttl))
        return self._invoke_on_key(request, key_data, map_set_ttl_codec.decode_response)

    def _try_remove_internal(self, key_data, timeout):
        request = map_try_remove_codec.encode_request(
            self.name, key_data, thread_id(), to_millis(timeout)
        )
        return self._invoke_on_key(request, key_data, map_try_remove_codec.decode_response)

    def _try_put_internal(self, key_data, value_data, timeout):
        request = map_try_put_codec.encode_request(
            self.name, key_data, value_data, thread_id(), to_millis(timeout)
        )
        return self._invoke_on_key(request, key_data, map_try_put_codec.decode_response)

    def _put_transient_internal(self, key_data, value_data, ttl, max_idle):
        if max_idle is not None:
            request = map_put_transient_with_max_idle_codec.encode_request(
                self.name, key_data, value_data, thread_id(), to_millis(ttl), to_millis(max_idle)
            )
        else:
            request = map_put_transient_codec.encode_request(
                self.name, key_data, value_data, thread_id(), to_millis(ttl)
            )
        return self._invoke_on_key(request, key_data)

    def _put_if_absent_internal(self, key_data, value_data, ttl, max_idle):
        def handler(message):
            return self._to_object(map_put_if_absent_codec.decode_response(message))

        if max_idle is not None:
            request = map_put_if_absent_with_max_idle_codec.encode_request(
                self.name, key_data, value_data, thread_id(), to_millis(ttl), to_millis(max_idle)
            )
        else:
            request = map_put_if_absent_codec.encode_request(
                self.name, key_data, value_data, thread_id(), to_millis(ttl)
            )
        return self._invoke_on_key(request, key_data, handler)

    def _replace_if_same_internal(self, key_data, old_value_data, new_value_data):
        request = map_replace_if_same_codec.encode_request(
            self.name, key_data, old_value_data, new_value_data, thread_id()
        )
        return self._invoke_on_key(request, key_data, map_replace_if_same_codec.decode_response)

    def _replace_internal(self, key_data, value_data):
        def handler(message):
            return self._to_object(map_replace_codec.decode_response(message))

        request = map_replace_codec.encode_request(self.name, key_data, value_data, thread_id())
        return self._invoke_on_key(request, key_data, handler)

    def _evict_internal(self, key_data):
        request = map_evict_codec.encode_request(self.name, key_data, thread_id())
        return self._invoke_on_key(request, key_data, map_evict_codec.decode_response)

    def _load_all_internal(self, key_data_list, replace_existing_values):
        request = map_load_given_keys_codec.encode_request(
            self.name, key_data_list, replace_existing_values
        )
        return self._invoke(request)

    def _execute_on_key_internal(self, key_data, entry_processor_data):
        def handler(message):
            return self._to_object(map_execute_on_key_codec.decode_response(message))

        request = map_execute_on_key_codec.encode_request(
            self.name, entry_processor_data, key_data, thread_id()
        )
        return self._invoke_on_key(request, key_data, handler)


def create_map_proxy(service_name, name, context):
    near_cache_config = context.config.near_caches.get(name, None)
    if near_cache_config is None:
        return Map(service_name, name, context)
    raise InvalidConfigurationError("near cache is not supported")
