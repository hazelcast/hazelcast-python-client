import typing
from uuid import uuid4
from hazelcast import future
from hazelcast.core import MemberInfo
from hazelcast.future import Future
from hazelcast.protocol.codec import (
    executor_service_shutdown_codec,
    executor_service_is_shutdown_codec,
    executor_service_submit_to_partition_codec,
    executor_service_submit_to_member_codec,
)
from hazelcast.proxy.base import Proxy
from hazelcast.serialization.compact import SchemaNotReplicatedError
from hazelcast.util import check_not_none


class Executor(Proxy["BlockingExecutor"]):
    """An object that executes submitted executable tasks."""

    def execute_on_key_owner(self, key: typing.Any, task: typing.Any) -> Future[typing.Any]:
        """Executes a task on the owner of the specified key.

        Args:
            key: The specified key.
            task: A task executed on the owner of the specified key.

        Returns:
            The result of the task.
        """
        check_not_none(key, "key can't be None")
        check_not_none(task, "task can't be None")

        try:
            key_data = self._to_data(key)
            task_data = self._to_data(task)
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.execute_on_key_owner, key, task)

        partition_id = self._context.partition_service.get_partition_id(key_data)
        uuid = uuid4()

        def handler(message):
            return self._to_object(
                executor_service_submit_to_partition_codec.decode_response(message)
            )

        request = executor_service_submit_to_partition_codec.encode_request(
            self.name, uuid, task_data
        )
        return self._invoke_on_partition(request, partition_id, handler)

    def execute_on_member(self, member: MemberInfo, task: typing.Any) -> Future[typing.Any]:
        """Executes a task on the specified member.

        Args:
            member: The specified member.
            task: The task executed on the specified member.

        Returns:
            The result of the task.
        """
        check_not_none(task, "task can't be None")
        try:
            task_data = self._to_data(task)
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.execute_on_member, member, task)

        uuid = uuid4()
        return self._execute_on_member(uuid, task_data, member.uuid)

    def execute_on_members(
        self, members: typing.Sequence[MemberInfo], task: typing.Any
    ) -> Future[typing.List[typing.Any]]:
        """Executes a task on each of the specified members.

        Args:
            members: The specified members.
            task: The task executed on the specified members.

        Returns:
            The list of results of the tasks on each member.
        """
        try:
            task_data = self._to_data(task)
        except SchemaNotReplicatedError as e:
            return self._send_schema_and_retry(e, self.execute_on_members, members, task)

        futures = []
        uuid = uuid4()
        for member in members:
            f = self._execute_on_member(uuid, task_data, member.uuid)
            futures.append(f)

        return future.combine_futures(futures)

    def execute_on_all_members(self, task: typing.Any) -> Future[typing.List[typing.Any]]:
        """Executes a task on all the known cluster members.

        Args:
            task: The task executed on the all the members.

        Returns:
            The list of results of the tasks on each member.
        """
        return self.execute_on_members(self._context.cluster_service.get_members(), task)

    def is_shutdown(self) -> Future[bool]:
        """Determines whether this executor has been shutdown or not.

        Returns:
            ``True`` if the executor has been shutdown, ``False`` otherwise.
        """
        request = executor_service_is_shutdown_codec.encode_request(self.name)
        return self._invoke(request, executor_service_is_shutdown_codec.decode_response)

    def shutdown(self) -> Future[None]:
        """Initiates a shutdown process which works orderly. Tasks that were
        submitted before shutdown are executed but new task will not be
        accepted.
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

    def blocking(self) -> "BlockingExecutor":
        return BlockingExecutor(self)


class BlockingExecutor(Executor):
    __slots__ = ("_wrapped", "name", "service_name")

    def __init__(self, wrapped: Executor):
        self.name = wrapped.name
        self.service_name = wrapped.service_name
        self._wrapped = wrapped

    def execute_on_key_owner(  # type: ignore[override]
        self,
        key: typing.Any,
        task: typing.Any,
    ) -> typing.Any:
        return self._wrapped.execute_on_key_owner(key, task).result()

    def execute_on_member(  # type: ignore[override]
        self,
        member: MemberInfo,
        task: typing.Any,
    ) -> typing.Any:
        return self._wrapped.execute_on_member(member, task).result()

    def execute_on_members(  # type: ignore[override]
        self,
        members: typing.Sequence[MemberInfo],
        task: typing.Any,
    ) -> typing.List[typing.Any]:
        return self._wrapped.execute_on_members(members, task).result()

    def execute_on_all_members(  # type: ignore[override]
        self,
        task: typing.Any,
    ) -> typing.List[typing.Any]:
        return self._wrapped.execute_on_all_members(task).result()

    def is_shutdown(  # type: ignore[override]
        self,
    ) -> bool:
        return self._wrapped.is_shutdown().result()

    def shutdown(  # type: ignore[override]
        self,
    ) -> None:
        return self._wrapped.shutdown().result()

    def blocking(self) -> "BlockingExecutor":
        return self

    def destroy(self) -> bool:
        return self._wrapped.destroy()

    def __repr__(self) -> str:
        return self._wrapped.__repr__()
