import itertools
import unittest

from mock import MagicMock

from hazelcast.errors import (
    HazelcastClientNotActiveError,
    ClientOfflineError,
    OperationTimeoutError,
    IllegalArgumentError,
    HazelcastInstanceNotActiveError,
    DistributedObjectDestroyedError,
)
from hazelcast.future import ImmediateFuture, ImmediateExceptionFuture, Future
from hazelcast.proxy import ReliableTopic
from hazelcast.proxy.reliable_topic import ReliableMessageListener


def no_op(_):
    pass


class ReliableTopicErrorTest(unittest.TestCase):
    def setUp(self):
        self.ringbuffer = MagicMock()
        self.ringbuffer.tail_sequence.return_value = ImmediateFuture(0)
        client = MagicMock()
        client.get_ringbuffer.return_value = self.ringbuffer
        context = MagicMock(client=client)
        self.topic = ReliableTopic("", "", context)

    def test_read_many_on_client_not_active_error(self):
        self.error_on_first_read_many(HazelcastClientNotActiveError("expected"))
        self.topic.add_listener(no_op)
        self.assertEqual(1, self.ringbuffer.read_many.call_count)
        self.assertEqual(0, len(self.topic._runners))

    def test_read_many_on_client_offline_error(self):
        self.error_on_first_read_many(ClientOfflineError())
        self.topic.add_listener(no_op)
        self.assertEqual(2, self.ringbuffer.read_many.call_count)
        self.assertEqual(1, len(self.topic._runners))

    def test_read_many_on_timeout_error(self):
        self.error_on_first_read_many(OperationTimeoutError("expected"))
        self.topic.add_listener(no_op)
        self.assertEqual(2, self.ringbuffer.read_many.call_count)
        self.assertEqual(1, len(self.topic._runners))

    def test_read_many_on_illegal_argument_error(self):
        self.error_on_first_read_many(IllegalArgumentError("expected"))
        self.topic.add_listener(no_op)
        self.assertEqual(1, self.ringbuffer.read_many.call_count)
        self.assertEqual(0, len(self.topic._runners))

    def test_read_many_on_illegal_argument_error_with_loss_tolerant_listener(self):
        self.error_on_first_read_many(IllegalArgumentError("expected"))
        self.ringbuffer.head_sequence.return_value = ImmediateFuture(0)

        class Listener(ReliableMessageListener):
            def on_message(self, message):
                pass

            def retrieve_initial_sequence(self):
                return 0

            def store_sequence(self, sequence):
                pass

            def is_loss_tolerant(self):
                return True

            def is_terminal(self, error):
                pass

        self.topic.add_listener(Listener())
        self.assertEqual(2, self.ringbuffer.read_many.call_count)
        self.assertEqual(1, len(self.topic._runners))

    def test_read_many_on_illegal_argument_error_with_loss_tolerant_listener_when_head_sequence_fails(
        self,
    ):
        self.error_on_first_read_many(IllegalArgumentError("expected"))
        self.ringbuffer.head_sequence.return_value = ImmediateExceptionFuture(
            RuntimeError("expected")
        )

        class Listener(ReliableMessageListener):
            def on_message(self, message):
                pass

            def retrieve_initial_sequence(self):
                return 0

            def store_sequence(self, sequence):
                pass

            def is_loss_tolerant(self):
                return True

            def is_terminal(self, error):
                pass

        self.topic.add_listener(Listener())
        self.assertEqual(1, self.ringbuffer.read_many.call_count)
        self.assertEqual(0, len(self.topic._runners))

    def test_read_many_on_instance_not_active_error(self):
        self.error_on_first_read_many(HazelcastInstanceNotActiveError("expected"))
        self.topic.add_listener(no_op)
        self.assertEqual(1, self.ringbuffer.read_many.call_count)
        self.assertEqual(0, len(self.topic._runners))

    def test_read_many_on_distributed_object_destroyed_error(self):
        self.error_on_first_read_many(DistributedObjectDestroyedError("expected"))
        self.topic.add_listener(no_op)
        self.assertEqual(1, self.ringbuffer.read_many.call_count)
        self.assertEqual(0, len(self.topic._runners))

    def test_read_many_on_generic_error(self):
        self.error_on_first_read_many(RuntimeError("expected"))
        self.topic.add_listener(no_op)
        self.assertEqual(1, self.ringbuffer.read_many.call_count)
        self.assertEqual(0, len(self.topic._runners))

    def error_on_first_read_many(self, error):
        counter = itertools.count()

        def read_many_mock(*_):
            if next(counter) == 0:
                return ImmediateExceptionFuture(error)
            return Future()

        self.ringbuffer.read_many.side_effect = read_many_mock
