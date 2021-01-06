from uuid import uuid4
from hazelcast import future
from hazelcast.protocol.codec import (
    executor_service_shutdown_codec,
    executor_service_is_shutdown_codec,
    executor_service_submit_to_partition_codec,
    executor_service_submit_to_member_codec,
)
from hazelcast.proxy.base import Proxy
from hazelcast.util import check_not_none


class Executor(Proxy):
    """An object that executes submitted executable tasks."""

    def execute_on_key_owner(self, key, task):
        """Executes a task on the owner of the specified key.

        Args:
          key: The specified key.
          task: A task executed on the owner of the specified key.

        Returns:
          hazelcast.future.Future: future representing pending completion of the task.
        """
        check_not_none(key, "key can't be None")
        check_not_none(task, "task can't be None")

        def handler(message):
            return self._to_object(
                executor_service_submit_to_partition_codec.decode_response(message)
            )

        key_data = self._to_data(key)
        task_data = self._to_data(task)

        partition_id = self._context.partition_service.get_partition_id(key_data)
        uuid = uuid4()
        request = executor_service_submit_to_partition_codec.encode_request(
            self.name, uuid, task_data
        )
        return self._invoke_on_partition(request, partition_id, handler)

    def execute_on_member(self, member, task):
        """Executes a task on the specified member.

        Args:
          member (hazelcast.core.MemberInfo): The specified member.
          task: The task executed on the specified member.

        Returns:
          hazelcast.future.Future: Future representing pending completion of the task.
        """
        check_not_none(task, "task can't be None")
        task_data = self._to_data(task)
        uuid = uuid4()
        return self._execute_on_member(uuid, task_data, member.uuid)

    def execute_on_members(self, members, task):
        """Executes a task on each of the specified members.

        Args:
          members (list[hazelcast.core.MemberInfo]): The specified members.
          task: The task executed on the specified members.

        Returns:
          list[hazelcast.future.Future]: Futures representing pending completion of the task on each member.

        """
        task_data = self._to_data(task)
        futures = []
        uuid = uuid4()
        for member in members:
            f = self._execute_on_member(uuid, task_data, member.uuid)
            futures.append(f)
        return future.combine_futures(futures)

    def execute_on_all_members(self, task):
        """Executes a task on all of the known cluster members.

        Args:
          task: The task executed on the all of the members.

        Returns:
          list[hazelcast.future.Future]: Futures representing pending completion of the task on each member.
        """
        return self.execute_on_members(self._context.cluster_service.get_members(), task)

    def is_shutdown(self):
        """Determines whether this executor has been shutdown or not.

        Returns:
            hazelcast.future.Future[bool]: ``True`` if the executor has been shutdown, ``False`` otherwise.
        """
        request = executor_service_is_shutdown_codec.encode_request(self.name)
        return self._invoke(request, executor_service_is_shutdown_codec.decode_response)

    def shutdown(self):
        """Initiates a shutdown process which works orderly. Tasks that were submitted
        before shutdown are executed but new task will not be accepted.

        Returns:
            hazelcast.future.Future[None]:
        """
        request = executor_service_shutdown_codec.encode_request(self.name)
        return self._invoke(request)

    def _execute_on_member(self, uuid, task_data, member_uuid):
        def handler(message):
            return self._to_object(executor_service_submit_to_member_codec.decode_response(message))

        request = executor_service_submit_to_member_codec.encode_request(
            self.name, uuid, task_data, member_uuid
        )
        return self._invoke_on_target(request, member_uuid, handler)
