import asyncio
import logging
import time
import typing
from uuid import uuid4

from hazelcast.config import ReliableTopicConfig, TopicOverloadPolicy
from hazelcast.core import MemberInfo, MemberVersion, EndpointQualifier, ProtocolType
from hazelcast.errors import (
    OperationTimeoutError,
    IllegalArgumentError,
    HazelcastClientNotActiveError,
    ClientOfflineError,
    HazelcastInstanceNotActiveError,
    DistributedObjectDestroyedError,
    TopicOverloadError,
)
from hazelcast.internal.asyncio_proxy.base import Proxy
from hazelcast.proxy.base import TopicMessage
from hazelcast.proxy.reliable_topic import ReliableMessageListener, _ReliableMessageListenerAdapter
from hazelcast.proxy.ringbuffer import OVERFLOW_POLICY_FAIL, OVERFLOW_POLICY_OVERWRITE
from hazelcast.serialization.compact import SchemaNotReplicatedError
from hazelcast.serialization.objects import ReliableTopicMessage
from hazelcast.types import MessageType
from hazelcast.util import check_not_none

_INITIAL_BACKOFF = 0.1
_MAX_BACKOFF = 2.0

_UNKNOWN_MEMBER_VERSION = MemberVersion(0, 0, 0)
_MEMBER_ENDPOINT_QUALIFIER = EndpointQualifier(ProtocolType.MEMBER, None)

_logger = logging.getLogger(__name__)


class _MessageRunner:
    def __init__(
        self,
        registration_id,
        listener,
        ringbuffer,
        topic_name,
        read_batch_size,
        to_object,
        runners,
    ):
        self._registration_id = registration_id
        self._listener = listener
        self._ringbuffer = ringbuffer
        self._topic_name = topic_name
        self._read_batch_size = read_batch_size
        self._to_object = to_object
        self._runners = runners
        self._sequence = listener.retrieve_initial_sequence()
        self._cancelled = False
        self._task: asyncio.Task | None = None

    async def start(self):
        """Starts the message runner by checking the given sequence.

        If the user provided an initial sequence via listener, we will
        use it as it is. If not, we will ask server to get the tail
        sequence and use it.
        """
        if self._sequence != -1:
            # User provided a sequence to start from
            return

        # We are going to listen to next publication.
        # We don't care about what already has been published.
        sequence = await self._ringbuffer.tail_sequence()
        self._sequence = sequence + 1

    def next_batch(self):
        """Schedules an asyncio task to read the next batch from the
        ringbuffer and call the listener on items when it is done.
        """
        if self._cancelled:
            return
        self._task = asyncio.create_task(self._run_next_batch())

    async def _run_next_batch(self):
        """Reads the next batch from the ringbuffer and processes the items."""
        if self._cancelled:
            return

        try:
            result = await self._ringbuffer.read_many(self._sequence, 1, self._read_batch_size)

            # Check if there are any messages lost since the last read
            # and whether the listener can tolerate that.
            lost_count = (result.next_sequence_to_read_from - result.read_count) - self._sequence
            if lost_count != 0 and not self._is_loss_tolerable(lost_count):
                self.cancel()
                return

            # Call the listener for each item read.
            for i in range(result.size):
                try:
                    message = result[i]
                    self._listener.store_sequence(result.get_sequence(i))

                    member = None
                    if message.publisher_address:
                        member = MemberInfo(
                            message.publisher_address,
                            None,
                            None,
                            False,
                            _UNKNOWN_MEMBER_VERSION,
                            None,
                            {
                                _MEMBER_ENDPOINT_QUALIFIER: message.publisher_address,
                            },
                        )

                    topic_message = TopicMessage(
                        self._topic_name,
                        message.payload,
                        message.publish_time,
                        member,
                    )
                    self._listener.on_message(topic_message)
                except Exception as e:
                    if self._terminate(e):
                        self.cancel()
                        return

            self._sequence = result.next_sequence_to_read_from
            self.next_batch()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            # read_many request failed.
            if not await self._handle_internal_error(e):
                self.cancel()

    def cancel(self):
        """Sets the cancelled flag, cancels the running task, and removes
        the runner registration.
        """
        self._cancelled = True
        self._runners.pop(self._registration_id, None)
        # if self._task is not None and not self._task.done():
        #     self._task.cancel()
        self._listener.on_cancel()

    def _is_loss_tolerable(self, loss_count: int) -> bool:
        """Called when message loss is detected.

        Checks if the listener is able to tolerate the loss.

        Args:
            loss_count: Number of lost messages.

        Returns:
            ``True`` if the listener may continue reading.
        """
        if self._listener.is_loss_tolerant():
            _logger.debug(
                "MessageListener %s on topic %s lost %s messages.",
                self._listener,
                self._topic_name,
                loss_count,
            )
            return True

        _logger.warning(
            "Terminating MessageListener %s on topic %s. "
            "Reason: The listener was too slow or the retention period of the message has been violated. "
            "%s messages lost.",
            self._listener,
            self._topic_name,
            loss_count,
        )
        return False

    def _terminate(self, error: Exception) -> bool:
        """Checks if we should terminate the listener based on the error
        received while calling on_message.

        Args:
            error: Error received while calling the listener.

        Returns:
            Should terminate the listener or not.
        """
        if self._cancelled:
            return True

        try:
            terminate = self._listener.is_terminal(error)
            if terminate:
                _logger.warning(
                    "Terminating MessageListener %s on topic %s. Reason: Unhandled exception.",
                    self._listener,
                    self._topic_name,
                    exc_info=error,
                )
            else:
                _logger.debug(
                    "MessageListener %s on topic %s ran into an error.",
                    self._listener,
                    self._topic_name,
                    exc_info=error,
                )
            return terminate
        except Exception as e:
            _logger.warning(
                "Terminating MessageListener %s on topic %s. "
                "Reason: Unhandled exception while calling is_terminal method",
                self._listener,
                self._topic_name,
                exc_info=e,
            )
            return True

    async def _handle_internal_error(self, error: Exception) -> bool:
        """Called when the read_many request fails.

        Based on the error we receive, we will act differently.

        If we can tolerate the error, we will call next_batch here.
        The reasoning behind is that, on some cases, we do not immediately
        call next_batch, but make a request to the server, and based on
        that, call next_batch.

        Args:
            error: The error we received.

        Returns:
            ``True`` if the error is handled internally. ``False`` otherwise.
            When ``False`` is returned, listener should be cancelled.
        """
        if isinstance(error, HazelcastClientNotActiveError):
            return self._handle_client_not_active_error()
        elif isinstance(error, ClientOfflineError):
            return self._handle_client_offline_error()
        elif isinstance(error, OperationTimeoutError):
            return self._handle_timeout_error()
        elif isinstance(error, IllegalArgumentError):
            return await self._handle_illegal_argument_error(error)
        elif isinstance(error, HazelcastInstanceNotActiveError):
            return self._handle_instance_not_active_error()
        elif isinstance(error, DistributedObjectDestroyedError):
            return self._handle_distributed_object_destroyed_error()
        else:
            return self._handle_generic_error(error)

    def _handle_generic_error(self, error):
        # Received an error we do not expect.
        _logger.warning(
            "Terminating MessageListener %s on topic %s. Reason: Unhandled exception.",
            self._listener,
            self._topic_name,
            exc_info=error,
        )
        return False

    def _handle_distributed_object_destroyed_error(self):
        # Underlying ringbuffer is destroyed. It should only
        # happen when the user destroys the reliable topic
        # associated with it.
        _logger.debug(
            "Terminating MessageListener %s on topic %s. Reason: Topic is destroyed.",
            self._listener,
            self._topic_name,
        )
        return False

    def _handle_instance_not_active_error(self):
        # This error should be received from the server.
        # We do not throw it anywhere on the client.
        _logger.debug(
            "Terminating MessageListener %s on topic %s. Reason: Server is shutting down.",
            self._listener,
            self._topic_name,
        )
        return False

    def _handle_client_offline_error(self):
        # Client is reconnecting to cluster.
        _logger.debug(
            "MessageListener %s on topic %s got error. "
            "Continuing from the last known sequence %s.",
            self._listener,
            self._topic_name,
            self._sequence,
        )
        self.next_batch()
        return True

    def _handle_client_not_active_error(self):
        # Client#shutdown is called.
        _logger.debug(
            "Terminating MessageListener %s on topic %s. Reason: Client is shutting down.",
            self._listener,
            self._topic_name,
        )
        return False

    def _handle_timeout_error(self):
        # read_many invocation to the server timed out.
        _logger.debug(
            "MessageListener %s on topic %s timed out. "
            "Continuing from the last known sequence %s.",
            self._listener,
            self._topic_name,
            self._sequence,
        )
        self.next_batch()
        return True

    async def _handle_illegal_argument_error(self, error):
        # Server sends this when it detects data loss
        # on the underlying ringbuffer.
        if self._listener.is_loss_tolerant():
            # Listener can tolerate message loss. Try to continue reading
            # after getting head sequence, and try to read from there.
            try:
                head_sequence = await self._ringbuffer.head_sequence()
                _logger.debug(
                    "MessageListener %s on topic %s requested a too large sequence. "
                    "Jumping from old sequence %s to sequence %s.",
                    self._listener,
                    self._topic_name,
                    self._sequence,
                    head_sequence,
                    exc_info=error,
                )
                self._sequence = head_sequence
                # We call next_batch only after getting the new head
                # sequence and updating our state with it.
                self.next_batch()
            except Exception as e:
                _logger.warning(
                    "Terminating MessageListener %s on topic %s. "
                    "Reason: After the ring buffer data related "
                    "to reliable topic is lost, client tried to get the "
                    "current head sequence to continue since the listener "
                    "is loss tolerant, but that request failed.",
                    self._listener,
                    self._topic_name,
                    exc_info=e,
                )
                # We said that we can handle that error so the listener
                # is not cancelled. But, we could not continue since
                # our request to the server failed. We should cancel
                # the listener.
                self.cancel()
            return True

        _logger.warning(
            "Terminating MessageListener %s on topic %s. "
            "Reason: Underlying ring buffer data related to reliable topic is lost.",
            self._listener,
            self._topic_name,
        )
        return False


class ReliableTopic(Proxy, typing.Generic[MessageType]):
    """Hazelcast provides distribution mechanism for publishing messages that
    are delivered to multiple subscribers, which is also known as a
    publish/subscribe (pub/sub) messaging model. Publish and subscriptions are
    cluster-wide. When a member subscribes for a topic, it is actually
    registering for messages published by any member in the cluster, including
    the new members joined after you added the listener.

    Messages are ordered, meaning that listeners(subscribers) will process the
    messages in the order they are actually published.

    Hazelcast's Reliable Topic uses the same Topic interface as a regular topic.
    The main difference is that Reliable Topic is backed up by the Ringbuffer
    data structure, a replicated but not partitioned data structure that stores
    its data in a ring-like structure.
    """

    def __init__(self, service_name, name, context, ringbuffer):
        super(ReliableTopic, self).__init__(service_name, name, context)

        config = context.config.reliable_topics.get(name, None)
        if config is None:
            config = ReliableTopicConfig()

        self._config = config
        self._ringbuffer = ringbuffer
        self._runners: typing.Dict[str, _MessageRunner] = {}

    async def publish(self, message: MessageType) -> None:
        """Publishes the message to all subscribers of this topic.

        Args:
            message: The message.
        """
        check_not_none(message, "Message cannot be None")
        try:
            payload = self._to_data(message)
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.publish, message)

        topic_message = ReliableTopicMessage(time.time(), None, payload)

        overload_policy = self._config.overload_policy
        if overload_policy == TopicOverloadPolicy.BLOCK:
            return await self._add_with_backoff(topic_message)
        elif overload_policy == TopicOverloadPolicy.ERROR:
            return await self._add_or_fail(topic_message)
        elif overload_policy == TopicOverloadPolicy.DISCARD_OLDEST:
            return await self._add_or_overwrite(topic_message)
        elif overload_policy == TopicOverloadPolicy.DISCARD_NEWEST:
            return await self._add_or_discard(topic_message)
        else:
            raise ValueError(f"Unexpected overload policy is passed {overload_policy}")

    async def publish_all(self, messages: typing.Sequence[MessageType]) -> None:
        """Publishes all messages to all subscribers of this topic.

        Args:
            messages: Messages to publish.
        """
        check_not_none(messages, "Messages cannot be None")
        try:
            topic_messages = []
            for message in messages:
                check_not_none(message, "Message cannot be None")
                payload = self._to_data(message)
                topic_messages.append(ReliableTopicMessage(time.time(), None, payload))
        except SchemaNotReplicatedError as e:
            return await self._send_schema_and_retry(e, self.publish_all, messages)

        overload_policy = self._config.overload_policy
        if overload_policy == TopicOverloadPolicy.BLOCK:
            return await self._add_messages_with_backoff(topic_messages)
        elif overload_policy == TopicOverloadPolicy.ERROR:
            return await self._add_messages_or_fail(topic_messages)
        elif overload_policy == TopicOverloadPolicy.DISCARD_OLDEST:
            return await self._add_messages_or_overwrite(topic_messages)
        elif overload_policy == TopicOverloadPolicy.DISCARD_NEWEST:
            return await self._add_messages_or_discard(topic_messages)
        else:
            raise ValueError(f"Unexpected overload policy is passed {overload_policy}")

    async def add_listener(
        self,
        listener: typing.Union[
            ReliableMessageListener, typing.Callable[[TopicMessage[MessageType]], None]
        ],
    ) -> str:
        """Subscribes to this reliable topic.

        It can be either a simple function or an instance of an
        :class:`ReliableMessageListener`. When a function is passed, a
        :class:`ReliableMessageListener` is created out of that with
        sensible default values.

        When a message is published, the
        :func:`ReliableMessageListener.on_message` method of the given
        listener (or the function passed) is called.

        More than one message listener can be added on one instance.

        Args:
            listener: Listener to add.

        Returns:
            The registration id.
        """
        check_not_none(listener, "None listener is not allowed")
        registration_id = str(uuid4())
        reliable_message_listener = self._to_reliable_message_listener(listener)
        runner = _MessageRunner(
            registration_id,
            reliable_message_listener,
            self._ringbuffer,
            self.name,
            self._config.read_batch_size,
            self._to_object,
            self._runners,
        )
        await runner.start()
        # If the runner started successfully, register it.
        self._runners[registration_id] = runner
        runner.next_batch()
        # ensure the runner is scheduled
        await asyncio.sleep(0)
        return registration_id

    async def remove_listener(self, registration_id: str) -> bool:
        """Stops receiving messages for the given message listener.

        If the given listener already removed, this method does nothing.

        Args:
            registration_id: ID of listener registration.

        Returns:
            ``True`` if registration is removed, ``False`` otherwise.
        """
        check_not_none(registration_id, "Registration id cannot be None")
        runner = self._runners.get(registration_id, None)
        if not runner:
            return False

        runner.cancel()
        return True

    async def destroy(self) -> bool:
        """Destroys underlying Proxy and RingBuffer instances."""
        for runner in list(self._runners.values()):
            runner.cancel()

        self._runners.clear()
        await super(ReliableTopic, self).destroy()
        return await self._ringbuffer.destroy()

    async def _add_or_fail(self, message):
        sequence_id = await self._ringbuffer.add(message, OVERFLOW_POLICY_FAIL)
        if sequence_id == -1:
            raise TopicOverloadError(
                "Failed to publish message %s on topic %s." % (message, self.name)
            )

    async def _add_messages_or_fail(self, messages):
        sequence_id = await self._ringbuffer.add_all(messages, OVERFLOW_POLICY_FAIL)
        if sequence_id == -1:
            raise TopicOverloadError("Failed to publish messages on topic %s." % self.name)

    async def _add_or_overwrite(self, message):
        await self._ringbuffer.add(message, OVERFLOW_POLICY_OVERWRITE)

    async def _add_messages_or_overwrite(self, messages):
        await self._ringbuffer.add_all(messages, OVERFLOW_POLICY_OVERWRITE)

    async def _add_or_discard(self, message):
        await self._ringbuffer.add(message, OVERFLOW_POLICY_FAIL)

    async def _add_messages_or_discard(self, messages):
        await self._ringbuffer.add_all(messages, OVERFLOW_POLICY_FAIL)

    async def _add_with_backoff(self, message):
        backoff = _INITIAL_BACKOFF
        while True:
            sequence_id = await self._ringbuffer.add(message, OVERFLOW_POLICY_FAIL)
            if sequence_id != -1:
                return
            await asyncio.sleep(backoff)
            backoff = min(_MAX_BACKOFF, 2 * backoff)

    async def _add_messages_with_backoff(self, messages):
        backoff = _INITIAL_BACKOFF
        while True:
            sequence_id = await self._ringbuffer.add_all(messages, OVERFLOW_POLICY_FAIL)
            if sequence_id != -1:
                return
            await asyncio.sleep(backoff)
            backoff = min(_MAX_BACKOFF, 2 * backoff)

    @staticmethod
    def _to_reliable_message_listener(listener):
        if isinstance(listener, ReliableMessageListener):
            return listener

        if not callable(listener):
            raise TypeError("Listener must be a callable")

        return _ReliableMessageListenerAdapter(listener)
