from uuid import uuid4
from hazelcast import future
from hazelcast.protocol.codec import executor_service_submit_to_address_codec, executor_service_shutdown_codec, \
    executor_service_is_shutdown_codec, executor_service_cancel_on_address_codec, \
    executor_service_cancel_on_partition_codec, executor_service_submit_to_partition_codec
from hazelcast.proxy.base import Proxy
from hazelcast.util import check_not_none


class Executor(Proxy):
    # TODO: cancellation
    def execute_on_key_owner(self, key, task):
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        partition_id = self._client.partition_service.get_partition_id(key_data)

        uuid = self._get_uuid()
        return self._encode_invoke_on_partition(executor_service_submit_to_partition_codec, partition_id,
                                                uuid=uuid, callable=self._to_data(task),
                                                partition_id=partition_id)

    def execute_on_member(self, member, task):
        uuid = self._get_uuid()
        address = member.address
        return self._execute_on_member(address, uuid, self._to_data(task))

    def execute_on_members(self, members, task):
        task_data = self._to_data(task)
        futures = []
        uuid = self._get_uuid()
        for member in members:
            f = self._execute_on_member(member.address, uuid, task_data)
            futures.append(f)
        return future.combine_futures(*futures)

    def execute_on_all_members(self, task):
        return self.execute_on_members(self._client.cluster.members, task)

    def is_shutdown(self):
        return self._encode_invoke(executor_service_is_shutdown_codec)

    def shutdown(self):
        return self._encode_invoke(executor_service_shutdown_codec)

    def _execute_on_member(self, address, uuid, task_data):
        return self._encode_invoke_on_target(executor_service_submit_to_address_codec, address, uuid=uuid,
                                             callable=task_data, address=address)

    def _get_uuid(self):
        return str(uuid4())
