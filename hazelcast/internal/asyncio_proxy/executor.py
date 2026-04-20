import asyncio
import typing
from uuid import uuid4

from hazelcast.core import MemberInfo
from hazelcast.protocol.codec import (
    executor_service_shutdown_codec,
    executor_service_is_shutdown_codec,
    executor_service_submit_to_partition_codec,
    executor_service_submit_to_member_codec,
)
from hazelcast.internal.asyncio_proxy.base import Proxy
from hazelcast.serialization.compact import SchemaNotReplicatedError
from hazelcast.util import check_not_none


class Executor(Proxy):
    """An object that executes submitted executable tasks."""

    async def execute_on_key_owner(self, key: typing.Any, task: typing.Any) -> typing.Any:
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
            return await self._send_schema_and_retry(e, self.execute_on_key_owner, key, task)

        partition_id = self._partition_service.get_partition_id(key_data)
        uuid = uuid4()

        def handler(message):
            return self._to_object(
                executor_service_submit_to_partition_codec.decode_response(message)
            )

        request = executor_service_submit_to_partition_codec.encode_request(
            self.name, uuid, task_data
        )
        return await self._ainvoke_on_partition(request, partition_id, handler)

    async def execute_on_member(self, member: MemberInfo, task: typing.Any) -> typing.Any:
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
            return await self._send_schema_and_retry(e, self.execute_on_member, member, task)

        uuid = uuid4()
        return await self._execute_on_member(uuid, task_data, member.uuid)

    async def execute_on_members(
        self, members: typing.Sequence[MemberInfo], task: typing.Any
    ) -> typing.List[typing.Any]:
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
            return await self._send_schema_and_retry(e, self.execute_on_members, members, task)

        tasks = []
        async with asyncio.TaskGroup() as tg:  # type: ignore[attr-defined]
            tasks = [
                tg.create_task(self._execute_on_member(uuid4(), task_data, member.uuid))
                for member in members
            ]
        return [task.result() for task in tasks]

    async def execute_on_all_members(self, task: typing.Any) -> typing.List[typing.Any]:
        """Executes a task on all the known cluster members.

        Args:
            task: The task executed on the all the members.

        Returns:
            The list of results of the tasks on each member.
        """
        return await self.execute_on_members(self._context.cluster_service.get_members(), task)

    async def is_shutdown(self) -> bool:
        """Determines whether this executor has been shutdown or not.

        Returns:
            ``True`` if the executor has been shutdown, ``False`` otherwise.
        """
        request = executor_service_is_shutdown_codec.encode_request(self.name)
        return await self._invoke(request, executor_service_is_shutdown_codec.decode_response)

    async def shutdown(self) -> None:
        """Initiates a shutdown process which works orderly. Tasks that were
        submitted before shutdown are executed but new task will not be
        accepted.
        """
        request = executor_service_shutdown_codec.encode_request(self.name)
        return await self._invoke(request)

    async def _execute_on_member(self, uuid, task_data, member_uuid) -> typing.Any:
        def handler(message):
            return self._to_object(executor_service_submit_to_member_codec.decode_response(message))

        request = executor_service_submit_to_member_codec.encode_request(
            self.name, uuid, task_data, member_uuid
        )
        return await self._ainvoke_on_target(request, member_uuid, handler)


async def create_executor_proxy(service_name, name, context):
    return Executor(service_name, name, context)
