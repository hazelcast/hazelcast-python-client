from uuid import uuid4
from hazelcast import future
from hazelcast.protocol.codec import executor_service_submit_to_address_codec, executor_service_shutdown_codec, \
    executor_service_is_shutdown_codec, executor_service_cancel_on_address_codec, \
    executor_service_cancel_on_partition_codec, executor_service_submit_to_partition_codec
from hazelcast.proxy.base import Proxy
from hazelcast.util import check_not_none


class Executor(Proxy):
    """
    An object that executes submitted executable tasks.
    """
    # TODO: cancellation
    def execute_on_key_owner(self, key, task):
        """
        Executes a task on the owner of the specified key.

        :param key: (object), the specified key.
        :param task: (Task), a task executed on the owner of the specified key.
        :return: (:class:`~hazelcast.future.Future`), future representing pending completion of the task.
        """
        check_not_none(key, "key can't be None")
        key_data = self._to_data(key)
        partition_id = self._client.partition_service.get_partition_id(key_data)

        uuid = self._get_uuid()
        return self._encode_invoke_on_partition(executor_service_submit_to_partition_codec, partition_id,
                                                uuid=uuid, callable=self._to_data(task),
                                                partition_id=partition_id)

    def execute_on_member(self, member, task):
        """
        Executes a task on the specified member.

        :param member: (Member), the specified member.
        :param task: (Task), the task executed on the specified member.
        :return: (:class:`~hazelcast.future.Future`), Future representing pending completion of the task.
        """
        uuid = self._get_uuid()
        address = member.address
        return self._execute_on_member(address, uuid, self._to_data(task))

    def execute_on_members(self, members, task):
        """
        Executes a task on each of the specified members.

        :param members: (Collection), the specified members.
        :param task: (Task), the task executed on the specified members.
        :return: (Map), :class:`~hazelcast.future.Future` tuples representing pending completion of the task on each member.
        """
        task_data = self._to_data(task)
        futures = []
        uuid = self._get_uuid()
        for member in members:
            f = self._execute_on_member(member.address, uuid, task_data)
            futures.append(f)
        return future.combine_futures(*futures)

    def execute_on_all_members(self, task):
        """
        Executes a task on all of the known cluster members.

        :param task: (Task), the task executed on the all of the members.
        :return: (Map), :class:`~hazelcast.future.Future` tuples representing pending completion of the task on each member.
        """
        return self.execute_on_members(self._client.cluster.members, task)

    def is_shutdown(self):
        """
        Determines whether this executor has been shut down or not.

        :return: (bool), ``true`` if this executor has been shut down.
        """
        return self._encode_invoke(executor_service_is_shutdown_codec)

    def shutdown(self):
        """
        Initiates a shutdown process which works orderly. Tasks that were submitted before shutdown are executed but new
        task will not be accepted.
        """
        return self._encode_invoke(executor_service_shutdown_codec)

    def _execute_on_member(self, address, uuid, task_data):
        return self._encode_invoke_on_target(executor_service_submit_to_address_codec, address, uuid=uuid,
                                             callable=task_data, address=address)

    def _get_uuid(self):
        return str(uuid4())
