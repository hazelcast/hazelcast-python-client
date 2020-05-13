import time
import threading
from uuid import uuid4

from hazelcast.config import ReliableTopicConfig, TOPIC_OVERLOAD_POLICY
from hazelcast.exception import IllegalArgumentError, TopicOverflowError, HazelcastInstanceNotActiveError, \
    HazelcastClientNotActiveException, DistributedObjectDestroyedError, StaleSequenceError, OperationTimeoutError
from hazelcast.proxy.base import Proxy, TopicMessage
from hazelcast.proxy.ringbuffer import OVERFLOW_POLICY_FAIL, OVERFLOW_POLICY_OVERWRITE
from hazelcast.serialization.reliable_topic import ReliableTopicMessage
from hazelcast.util import current_time_in_millis
from hazelcast.six.moves import queue

_INITIAL_BACKOFF = 0.1
_MAX_BACKOFF = 2


class ReliableMessageListener(object):
    def on_message(self, item):
        """
        Invoked when a message is received for the added reliable topic.

        :param: message the message that is received for the added reliable topic
        """
        raise NotImplementedError

    def retrieve_initial_sequence(self):
        """
        Retrieves the initial sequence from which this ReliableMessageListener
        should start.

        Return -1 if there is no initial sequence and you want to start
        from the next published message.

        If you intend to create a durable subscriber so you continue from where
        you stopped the previous time, load the previous sequence and add 1.
        If you don't add one, then you will be receiving the same message twice.

        :return: (int), the initial sequence
        """
        return -1

    def store_sequence(self, sequence):
        """"
        Informs the ReliableMessageListener that it should store the sequence.
        This method is called before the message is processed. Can be used to
        make a durable subscription.

        :param: (int) ``sequence`` the sequence
        """
        pass

    def is_loss_tolerant(self):
        """
        Checks if this ReliableMessageListener is able to deal with message loss.
        Even though the reliable topic promises to be reliable, it can be that a
        MessageListener is too slow. Eventually the message won't be available
        anymore.

        If the ReliableMessageListener is not loss tolerant and the topic detects
        that there are missing messages, it will terminate the
        ReliableMessageListener.

        :return: (bool) ``True`` if the ReliableMessageListener is tolerant towards losing messages.
        """
        return False

    def is_terminal(self):
        """
        Checks if the ReliableMessageListener should be terminated based on an
        exception thrown while calling on_message.

        :return: (bool) ``True` if the ReliableMessageListener should terminate itself, ``False`` if it should keep on running.
        """
        raise False


class _MessageListener(object):
    def __init__(self, uuid, proxy, to_object, listener):
        self._id = uuid
        self._proxy = proxy
        self._to_object = to_object
        self._listener = listener
        self._cancelled_lock = threading.Lock()
        self._cancelled = False
        self._sequence = 0
        self._q = queue.Queue()

    def start(self):
        tail_seq = self._proxy.ringbuffer.tail_sequence()
        initial_seq = self._listener.retrieve_initial_sequence()
        if initial_seq == -1:
            initial_seq = tail_seq.result() + 1
        self._sequence = initial_seq
        self._proxy.client.reactor.add_timer(0, self._next)

    def _handle_illegal_argument_error(self):
        head_seq = self._proxy.ringbuffer.head_sequence().result()
        self._proxy.logger.warning("MessageListener {} on topic {} requested a too large sequence. Jumping from old "
                                   "sequence: {} to sequence: {}".format(self._id, self._proxy.name, self._sequence,
                                                                         head_seq))
        self._sequence = head_seq
        self._next()

    def _handle_stale_sequence_error(self):
        head_seq = self._proxy.ringbuffer.head_sequence().result()
        if self._listener.is_loss_tolerant:
            self._sequence = head_seq
            self._proxy.logger.warning("Topic {} ran into a stale sequence. Jumping from old sequence {} to new "
                                       "sequence {}".format(self._proxy.name, self._sequence, head_seq))
            self._next()
            return True

        self._proxy.logger.warning(
            "Terminating Message Listener: {} on topic: {}. Reason: The listener was too slow or the retention "
            "period of the message has been violated. Head: {}, sequence: {}".format(self._id, self._proxy.name,
                                                                                     head_seq, self._sequence))
        return False

    def _handle_operation_timeout_error(self):
        self._proxy.logger.info("Message Listener ", self._proxy.id, "on topic: ", self._proxy.name, " timed out. " +
                                "Continuing from the last known sequence ", self._proxy.sequence)
        self._next()

    def _handle_exception(self, exception):
        base_msg = "Terminating Message Listener: " + self._id + " on topic: " + self._proxy.name + ". Reason: "
        if isinstance(exception, IllegalArgumentError) and self._listener.is_loss_tolerant():
            self._handle_illegal_argument_error()
            return
        elif isinstance(exception, StaleSequenceError):
            if self._handle_stale_sequence_error():
                return
        elif isinstance(exception, OperationTimeoutError):
            self._handle_operation_timeout_error()
            return
        elif isinstance(exception, HazelcastInstanceNotActiveError):
            self._proxy.logger.info(base_msg + "HazelcastInstance is shutting down.")
        elif isinstance(exception, HazelcastClientNotActiveException):
            self._proxy.logger.info(base_msg + "HazelcastClient is shutting down.")
        elif isinstance(exception, DistributedObjectDestroyedError):
            self._proxy.logger.info(base_msg + "ReliableTopic is destroyed.")
        else:
            self._proxy.logger.warning(base_msg + "Unhandled error, message: " + str(exception))

        self._cancel_and_remove_listener()

    def _terminate(self, exception):
        with self._cancelled_lock:
            if self._cancelled:
                return True

        base_msg = "Terminating Message Listener: {} on topic: {}. Reason: ".format(self._id, self._proxy.name)
        try:
            terminate = self._listener.is_terminal()
            if terminate:
                self._proxy.logger.warning(base_msg + "Unhandled error: {}".format(str(exception)))
                return True

            self._proxy.logger.warning("MessageListener {} on topic: {} ran into an error: {}".
                                       format(self._id, self._proxy.name, str(exception)))
            return False

        except Exception as e:
            self._proxy.logger.warning(base_msg + "Unhandled error while calling ReliableMessageListener.is_terminal() "
                                                  "method: {}".format(str(e)))
            return True

    def _process(self, msg):
        try:
            self._listener.on_message(msg)
        except BaseException as e:
            if self._terminate(e):
                self._cancel_and_remove_listener()

    def _on_response(self, res):
        try:
            for message in res.result():
                with self._cancelled_lock:
                    if self._cancelled:
                        return

                msg = TopicMessage(
                    self._proxy.name,
                    message.payload,
                    message.publish_time,
                    message.publisher_address,
                    self._to_object
                )
                self._listener.store_sequence(self._sequence)
                self._process(msg)
                self._sequence += 1

            # Await for new messages
            self._next()
        except Exception as e:
            self._handle_exception(e)

    def _next(self):
        def _read_many():
            with self._cancelled_lock:
                if self._cancelled:
                    return

            future = self._proxy.ringbuffer.read_many(self._sequence, 1, self._proxy.config.read_batch_size)
            future.continue_with(self._on_response)

        self._proxy.client.reactor.add_timer(0, _read_many)

    def cancel(self):
        with self._cancelled_lock:
            self._cancelled = True

    def _cancel_and_remove_listener(self):
        try:
            # _proxy.remove_listener calls listener.cancel function
            self._proxy.remove_listener(self._id)
        except IllegalArgumentError as e:
            # This listener is already removed
            self._proxy.logger.debug("Failed to remove listener. Reason: {}".format(str(e)))


class ReliableTopic(Proxy):
    """
    Hazelcast provides distribution mechanism for publishing messages that are delivered to multiple subscribers, which
    is also known as a publish/subscribe (pub/sub) messaging model. Publish and subscriptions are cluster-wide. When a
    member subscribes for a topic, it is actually registering for messages published by any member in the cluster,
    including the new members joined after you added the listener.

    Messages are ordered, meaning that listeners(subscribers) will process the messages in the order they are actually
    published.

    Hazelcast's Reliable Topic uses the same Topic interface as a regular topic. The main difference is that Reliable
    Topic is backed up by the Ringbuffer data structure, a replicated but not partitioned data structure that stores
    its data in a ring-like structure.
    """

    def __init__(self, client, service_name, name):
        super(ReliableTopic, self).__init__(client, service_name, name)

        config = client.config.reliable_topic_configs.get(name, None)
        if config is None:
            config = ReliableTopicConfig()

        self.client = client
        self.config = config
        self._topic_overload_policy = self.config.topic_overload_policy
        self.ringbuffer = client.get_ringbuffer("_hz_rb_" + name)
        self._message_listeners_lock = threading.RLock()
        self._message_listeners = {}

    def add_listener(self, reliable_topic_listener):
        """
        Subscribes to this reliable topic. When someone publishes a message on this topic, on_message() method of
        ReliableTopicListener is called.

        :param ReliableTopicListener: (Class), class to be used when a message is published.
        :return: (str), a registration id which is used as a key to remove the listener.
        """
        if not isinstance(reliable_topic_listener, ReliableMessageListener):
            raise IllegalArgumentError("Message listener is not an instance of ReliableTopicListener")

        registration_id = str(uuid4())
        listener = _MessageListener(registration_id, self, self._to_object, reliable_topic_listener)
        with self._message_listeners_lock:
            self._message_listeners[registration_id] = listener

        listener.start()
        return registration_id

    def _add_with_backoff(self, item):
        sleep_time = _INITIAL_BACKOFF
        while True:
            seq_id = self.ringbuffer.add(item, overflow_policy=OVERFLOW_POLICY_FAIL).result()
            if seq_id != -1:
                return
            time.sleep(sleep_time)
            sleep_time *= 2
            if sleep_time > _MAX_BACKOFF:
                sleep_time = _MAX_BACKOFF

    def _add_or_fail(self, item):
        seq_id = self.ringbuffer.add(item, overflow_policy=OVERFLOW_POLICY_FAIL).result()
        if seq_id == -1:
            raise TopicOverflowError("failed to publish message to topic: " + self.name)

    def publish(self, message):
        """
        Publishes the message to all subscribers of this topic

        :param message: (object), the message to be published.
        """
        # TODO: We need a publisher_address?
        item = ReliableTopicMessage(
            publish_time=current_time_in_millis(),
            publisher_address="",
            payload=self._to_data(message)
        )
        if self._topic_overload_policy == TOPIC_OVERLOAD_POLICY.BLOCK:
            self._add_with_backoff(item)
        elif self._topic_overload_policy == TOPIC_OVERLOAD_POLICY.ERROR:
            self._add_or_fail(item)
        elif self._topic_overload_policy == TOPIC_OVERLOAD_POLICY.DISCARD_NEWEST:
            self.ringbuffer.add(item, overflow_policy=OVERFLOW_POLICY_FAIL).result()
        elif self._topic_overload_policy == TOPIC_OVERLOAD_POLICY.DISCARD_OLDEST:
            self.ringbuffer.add(item, overflow_policy=OVERFLOW_POLICY_OVERWRITE).result()

    def remove_listener(self, registration_id):
        """
        Stops receiving messages for the given message listener. If the given listener already removed, this method does
        nothing.

        :param registration_id: (str), registration id of the listener to be removed.
        :return: (bool), ``true`` if the listener is removed, ``false`` otherwise.
        """
        with self._message_listeners_lock:
            if registration_id not in self._message_listeners:
                raise IllegalArgumentError("no listener is found with the given id : {}".format(registration_id))

            listener = self._message_listeners[registration_id]
            listener.cancel()
            del self._message_listeners[registration_id]
            return True

        return False

    def destroy(self):
        """
        Destroys underlying Proxy and RingBuffer instances
        """
        super(ReliableTopic, self).destroy()
        return self.ringbuffer.destroy()
