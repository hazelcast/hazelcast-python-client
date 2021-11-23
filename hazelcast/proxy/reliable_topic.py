import logging
import time
from uuid import uuid4

from hazelcast.config import _ReliableTopicConfig, TopicOverloadPolicy
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
from hazelcast.future import ImmediateFuture, Future
from hazelcast.proxy.base import Proxy, TopicMessage
from hazelcast.proxy.ringbuffer import OVERFLOW_POLICY_FAIL, OVERFLOW_POLICY_OVERWRITE
from hazelcast.serialization.objects import ReliableTopicMessage
from hazelcast.util import check_not_none

_INITIAL_BACKOFF = 0.1
_MAX_BACKOFF = 2.0

_RINGBUFFER_PREFIX = "_hz_rb_"

_UNKNOWN_MEMBER_VERSION = MemberVersion(0, 0, 0)
_MEMBER_ENDPOINT_QUALIFIER = EndpointQualifier(ProtocolType.MEMBER, None)

_logger = logging.getLogger(__name__)


class ReliableMessageListener(object):
    """A message listener for :class:`ReliableTopic`.

    A message listener will not be called concurrently (provided that it's
    not registered twice). So there is no need to synchronize access to
    the state it reads or writes.

    If a regular function is registered on a reliable topic, the message
    listener works fine, but it can't do much more than listen to messages.

    This is an enhanced version of that to better integrate with the reliable
    topic.

    **Durable Subscription**

    The ReliableMessageListener allows you to control where you want to start
    processing a message when the listener is registered. This makes it
    possible to create a durable subscription by storing the sequence of the
    last message and using this as the sequence id to start from.

    **Error handling**

    The ReliableMessageListener also gives the ability to deal with errors
    using the :func:`is_terminal` method. If a plain function is used, then it
    won't terminate on errors and it will keep on running. But in some
    cases it is better to stop running.

    **Global order**

    The ReliableMessageListener will always get all events in order (global
    order). It will not get duplicates and there will only be gaps if it is
    too slow. For more information see :func:`is_loss_tolerant`.

    **Delivery guarantees**

    Because the ReliableMessageListener controls which item it wants to
    continue from upon restart, it is very easy to provide an at-least-once
    or at-most-once delivery guarantee. The :func:`store_sequence` is always
    called before a message is processed; so it can be persisted on some
    non-volatile storage. When the :func:`retrieve_initial_sequence` returns
    the stored sequence, then an at-least-once delivery is implemented since
    the same item is now being processed twice. To implement an at-most-once
    delivery guarantee, add 1 to the stored sequence when the
    :func:`retrieve_initial_sequence` is called.
    """

    def on_message(self, message):
        """
        Invoked when a message is received for the added reliable topic.

        One should not block in this callback. If blocking is necessary,
        consider delegating that task to an executor or a thread pool.

        Args:
            message (hazelcast.proxy.base.TopicMessage): The message that
                is received for the topic
        """
        raise NotImplementedError("on_message")

    def retrieve_initial_sequence(self):
        """
        Retrieves the initial sequence from which this ReliableMessageListener
        should start.

        Return ``-1`` if there is no initial sequence and you want to start
        from the next published message.

        If you intend to create a durable subscriber so you continue from where
        you stopped the previous time, load the previous sequence and add ``1``.
        If you don't add one, then you will be receiving the same message twice.

        Returns:
            int: The initial sequence.
        """
        raise NotImplementedError("retrieve_initial_sequence")

    def store_sequence(self, sequence):
        """
        Informs the ReliableMessageListener that it should store the sequence.
        This method is called before the message is processed. Can be used to
        make a durable subscription.

        Args:
            sequence (int): The sequence.
        """
        raise NotImplementedError("store_sequence")

    def is_loss_tolerant(self):
        """
        Checks if this ReliableMessageListener is able to deal with message loss.
        Even though the reliable topic promises to be reliable, it can be that a
        ReliableMessageListener is too slow. Eventually the message won't be
        available anymore.

        If the ReliableMessageListener is not loss tolerant and the topic detects
        that there are missing messages, it will terminate the
        ReliableMessageListener.

        Returns:
            bool: ``True`` if the ReliableMessageListener is tolerant towards
            losing messages.
        """
        raise NotImplementedError("is_loss_tolerant")

    def is_terminal(self, error):
        """
        Checks if the ReliableMessageListener should be terminated based on an
        error raised while calling :func:`on_message`.

        Args:
            error (Exception): The error raised while calling
                :func:`on_message`

        Returns:
            bool: ``True`` if the ReliableMessageListener should terminate itself,
            ``False`` if it should keep on running.
        """
        raise NotImplementedError("is_terminal")


class _MessageRunner(object):
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

    def start(self):
        """Starts the message runner by checking the given sequence.

        If the user provided a initial sequence via listener, we will
        use it as it is. If not, we will ask server to get the tail
        sequence and use it.

        Returns:
            hazelcast.future.Future[None]:
        """
        if self._sequence != -1:
            # User provided a sequence to start from
            return ImmediateFuture(None)

        def continuation(future):
            sequence = future.result()
            self._sequence = sequence + 1

        # We are going to listen to next publication.
        # We don't care about what already has been published.
        return self._ringbuffer.tail_sequence().continue_with(continuation)

    def next_batch(self):
        """Tries to read the next batch from the ringbuffer
        and call the listener on items when it is done.
        """
        if self._cancelled:
            return

        self._ringbuffer.read_many(self._sequence, 1, self._read_batch_size).add_done_callback(
            self._handle_next_batch
        )

    def cancel(self):
        """Sets the cancelled flag and removes
        the runner registration.
        """
        self._cancelled = True
        self._runners.pop(self._registration_id, None)

    def _handle_next_batch(self, future):
        """Handles the result of the read_many request from
        the ringbuffer.

        Args:
            future (hazelcast.future.Future):
        """
        if self._cancelled:
            return

        try:
            result = future.result()

            # Check if there are any messages lost since the last read
            # and whether or not the listener can tolerate that.
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
                        self._to_object,
                    )
                    self._listener.on_message(topic_message)
                except Exception as e:
                    if self._terminate(e):
                        self.cancel()
                        return

            self._sequence = result.next_sequence_to_read_from
            self.next_batch()
        except Exception as e:
            # read_many request failed.
            if not self._handle_internal_error(e):
                self.cancel()

    def _is_loss_tolerable(self, loss_count):
        """Called when message loss is detected.

        Checks if the listener is able to tolerate the loss.

        Args:
            loss_count (int): Number of lost messages.

        Returns:
            bool: ``True`` if the listener may continue reading.
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

    def _terminate(self, error):
        """Checks if we should terminate the listener
        based on the error we received while calling the
        on_message for this message.

        If the listener says that it should be terminated
        for this error or it raises some error while
        we were trying to call is_terminal, the listener
        will be terminated. Otherwise, a log will be
        printed and listener will continue.

        Args:
            error (Exception): Error we received while
                calling the listener.

        Returns:
            bool: Should terminate the listener or not.
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

    def _handle_internal_error(self, error):
        """Called when the read_many request is failed.

        Based on the error we receive, we will act differently.

        If we can tolerate the error, we will call next_batch
        here. The reasoning behind is that, on some cases, we
        do not immediately call next_batch, but make a request
        to the server, and based on that, call next_batch.

        Args:
            error (Exception): The error we received.

        Returns:
            bool: ``True`` if the error is handled internally.
            ``False`` otherwise. When, ``False`` is returned,
            listener should be cancelled.
        """
        if isinstance(error, HazelcastClientNotActiveError):
            return self._handle_client_not_active_error()
        elif isinstance(error, ClientOfflineError):
            return self._handle_client_offline_error()
        elif isinstance(error, OperationTimeoutError):
            return self._handle_timeout_error()
        elif isinstance(error, IllegalArgumentError):
            return self._handle_illegal_argument_error(error)
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

    def _handle_illegal_argument_error(self, error):
        # Server sends this when it detects data loss
        # on the underlying ringbuffer.
        if self._listener.is_loss_tolerant():
            # Listener can tolerate message loss. Try
            # to continue reading after getting head
            # sequence, and try to read from there.
            def on_response(future):
                try:
                    head_sequence = future.result()
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
                    # sequence and updating the our state with it.
                    self.next_batch()
                except Exception as e:
                    _logger.warning(
                        "Terminating MessageListener %s on topic %s. "
                        "Reason: After the ring buffer data related "
                        "to reliable topic is lost, client tried to get the "
                        "current head sequence to continue since the listener"
                        "is loss tolerant, but that request failed.",
                        self._listener,
                        self._topic_name,
                        exc_info=e,
                    )
                    # We said that we can handle that error so the listener
                    # is not cancelled. But, we could not continue since
                    # our request to the server is failed. We should cancel
                    # the listener.
                    self.cancel()

            self._ringbuffer.head_sequence().add_done_callback(on_response)
            return True

        _logger.warning(
            "Terminating MessageListener %s on topic %s. "
            "Reason: Underlying ring buffer data related to reliable topic is lost.",
            self._listener,
            self._topic_name,
        )

        return False


class _ReliableMessageListenerAdapter(ReliableMessageListener):
    """Used when the user provided a function as the listener.

    That means user does not care about the other properties
    of the listener. They just want to listen messages. Fill
    the methods with expected defaults.
    """

    def __init__(self, on_message_fn):
        self._on_message_fn = on_message_fn

    def on_message(self, message):
        self._on_message_fn(message)

    def retrieve_initial_sequence(self):
        # -1 indicates start from next message.
        return -1

    def store_sequence(self, sequence):
        # no-op
        pass

    def is_loss_tolerant(self):
        # terminate the listener on message loss
        return False

    def is_terminal(self, error):
        # do not terminate the listener or errors
        return False


def _no_op_continuation(future):
    # Used when we just care about whether
    # the ringbuffer request is failed. We just
    # check the result and return nothing.
    future.result()


class ReliableTopic(Proxy):
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

    def __init__(self, service_name, name, context):
        super(ReliableTopic, self).__init__(service_name, name, context)

        config = context.config.reliable_topics.get(name, None)
        if config is None:
            config = _ReliableTopicConfig()

        self._config = config
        self._context = context
        self._ringbuffer = context.client.get_ringbuffer(_RINGBUFFER_PREFIX + name)
        self._runners = {}

    def publish(self, message):
        """Publishes the message to all subscribers of this topic.

        Args:
            message: The message.

        Returns:
            hazelcast.future.Future[None]:
        """
        check_not_none(message, "Message cannot be None")

        payload = self._to_data(message)
        topic_message = ReliableTopicMessage(time.time(), None, payload)

        overload_policy = self._config.overload_policy
        if overload_policy == TopicOverloadPolicy.BLOCK:
            return self._add_with_backoff(topic_message)
        elif overload_policy == TopicOverloadPolicy.ERROR:
            return self._add_or_fail(topic_message)
        elif overload_policy == TopicOverloadPolicy.DISCARD_OLDEST:
            return self._add_or_overwrite(topic_message)
        elif overload_policy == TopicOverloadPolicy.DISCARD_NEWEST:
            return self._add_or_discard(topic_message)

    def publish_all(self, messages):
        """Publishes all messages to all subscribers of this topic.

        Args:
            messages (list): Messages to publish.

        Returns:
            hazelcast.future.Future[None]:
        """
        check_not_none(messages, "Messages cannot be None")

        topic_messages = []

        for message in messages:
            check_not_none(message, "Message cannot be None")
            payload = self._to_data(message)
            topic_messages.append(ReliableTopicMessage(time.time(), None, payload))

        overload_policy = self._config.overload_policy
        if overload_policy == TopicOverloadPolicy.BLOCK:
            return self._add_messages_with_backoff(topic_messages)
        elif overload_policy == TopicOverloadPolicy.ERROR:
            return self._add_messages_or_fail(topic_messages)
        elif overload_policy == TopicOverloadPolicy.DISCARD_OLDEST:
            return self._add_messages_or_overwrite(topic_messages)
        elif overload_policy == TopicOverloadPolicy.DISCARD_NEWEST:
            return self._add_messages_or_discard(topic_messages)

    def add_listener(self, listener):
        """Subscribes to this reliable topic.

        It can be either a simple function or an instance of an
        :class:`ReliableMessageListener`. When a function is passed, a
        :class:`ReliableMessageListener` is created out of that with
        sensible default values.

        When a message is published, the,
        :func:`ReliableMessageListener.on_message` method of the given
        listener (or the function passed) is called.

        More than one message listener can be added on one instance.

        Args:
            listener (function or ReliableMessageListener): Listener to add.

        Returns:
            hazelcast.future.Future[str]: The registration id.
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

        def continuation(future):
            future.result()
            # If the runner started successfully, register it.
            self._runners[registration_id] = runner
            runner.next_batch()
            return registration_id

        return runner.start().continue_with(continuation)

    def remove_listener(self, registration_id):
        """Stops receiving messages for the given message listener.

        If the given listener already removed, this method does nothing.

        Args:
            registration_id (str): ID of listener registration.

        Returns:
            hazelcast.future.Future[bool]: ``True`` if registration is
            removed, ``False`` otherwise.
        """
        check_not_none(registration_id, "Registration id cannot be None")
        runner = self._runners.get(registration_id, None)
        if not runner:
            return ImmediateFuture(False)

        runner.cancel()
        return ImmediateFuture(True)

    def destroy(self):
        """
        Destroys underlying Proxy and RingBuffer instances.
        """

        for runner in list(self._runners.values()):
            runner.cancel()

        self._runners.clear()

        super(ReliableTopic, self).destroy()
        return self._ringbuffer.destroy()

    def _add_or_fail(self, message):
        def continuation(future):
            sequence_id = future.result()
            if sequence_id == -1:
                raise TopicOverloadError(
                    "Failed to publish message %s on topic %s." % (message, self.name)
                )

        return self._ringbuffer.add(message, OVERFLOW_POLICY_FAIL).continue_with(continuation)

    def _add_messages_or_fail(self, messages):
        def continuation(future):
            sequence_id = future.result()
            if sequence_id == -1:
                raise TopicOverloadError("Failed to publish messages on topic %s." % self.name)

        return self._ringbuffer.add_all(messages, OVERFLOW_POLICY_FAIL).continue_with(continuation)

    def _add_or_overwrite(self, message):
        return self._ringbuffer.add(message, OVERFLOW_POLICY_OVERWRITE).continue_with(
            _no_op_continuation
        )

    def _add_messages_or_overwrite(self, messages):
        return self._ringbuffer.add_all(messages, OVERFLOW_POLICY_OVERWRITE).continue_with(
            _no_op_continuation
        )

    def _add_or_discard(self, message):
        return self._ringbuffer.add(message, OVERFLOW_POLICY_FAIL).continue_with(
            _no_op_continuation
        )

    def _add_messages_or_discard(self, messages):
        return self._ringbuffer.add_all(messages, OVERFLOW_POLICY_FAIL).continue_with(
            _no_op_continuation
        )

    def _add_with_backoff(self, message):
        future = Future()
        self._try_adding_with_backoff(message, _INITIAL_BACKOFF, future)
        return future

    def _add_messages_with_backoff(self, messages):
        future = Future()
        self._try_adding_messages_with_backoff(messages, _INITIAL_BACKOFF, future)
        return future

    def _try_adding_with_backoff(self, message, backoff, future):
        def callback(add_future):
            try:
                sequence_id = add_future.result()
                if sequence_id != -1:
                    future.set_result(None)
                    return

                self._context.reactor.add_timer(
                    backoff,
                    lambda: self._try_adding_with_backoff(
                        message, min(_MAX_BACKOFF, 2 * backoff), future
                    ),
                )
            except Exception as e:
                future.set_result(e)

        self._ringbuffer.add(message, OVERFLOW_POLICY_FAIL).add_done_callback(callback)

    def _try_adding_messages_with_backoff(self, messages, backoff, future):
        def callback(add_future):
            try:
                sequence_id = add_future.result()
                if sequence_id != -1:
                    future.set_result(None)
                    return

                self._context.reactor.add_timer(
                    backoff,
                    lambda: self._try_adding_messages_with_backoff(
                        messages, min(_MAX_BACKOFF, 2 * backoff), future
                    ),
                )
            except Exception as e:
                future.set_result(e)

        self._ringbuffer.add_all(messages, OVERFLOW_POLICY_FAIL).add_done_callback(callback)

    @staticmethod
    def _to_reliable_message_listener(listener):
        if isinstance(listener, ReliableMessageListener):
            return listener

        if not callable(listener):
            raise TypeError("Listener must be a callable")

        return _ReliableMessageListenerAdapter(listener)
