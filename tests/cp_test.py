import time
import unittest

from mock import MagicMock

from hazelcast.cp import _SessionState, ProxySessionManager
from hazelcast.errors import HazelcastClientNotActiveError, SessionExpiredError
from hazelcast.future import ImmediateFuture, ImmediateExceptionFuture
from hazelcast.protocol import RaftGroupId
from hazelcast.reactor import AsyncoreReactor
from hazelcast.util import thread_id


class SessionStateTest(unittest.TestCase):
    def setUp(self):
        self.state = _SessionState(42, None, 0.5)

    def test_acquire(self):
        self.assertEqual(0, self.state.acquire_count.get())
        self.assertEqual(42, self.state.acquire(5))  # session id
        self.assertEqual(5, self.state.acquire_count.get())

    def test_release(self):
        self.state.acquire(5)
        self.state.release(4)
        self.assertEqual(1, self.state.acquire_count.get())

    def test_is_in_use(self):
        self.assertFalse(self.state.is_in_use())
        self.state.acquire(5)
        self.assertTrue(self.state.is_in_use())

    def test_is_valid(self):
        self.assertTrue(self.state.is_valid())  # not timed out
        time.sleep(1)
        self.assertFalse(self.state.is_valid())  # timed out and there is no acquire
        self.state.acquire(5)
        self.assertTrue(self.state.is_valid())  # timed out but acquired


class SessionManagerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.group_id = 42
        cls.session_id = 24
        cls.raft_group_id = RaftGroupId("test", 0, cls.group_id)

    def setUp(self):
        self.context = MagicMock()
        self.manager = ProxySessionManager(self.context)

    def tearDown(self):
        self.manager._sessions.clear()
        self.manager.shutdown().result()

    def test_get_session_id(self):
        self.assertEqual(-1, self.manager.get_session_id(self.raft_group_id))
        self.set_session(self.prepare_state())
        self.assertEqual(self.session_id, self.manager.get_session_id(self.raft_group_id))

    def test_acquire_session_after_shutdown(self):
        self.manager.shutdown().result()

        with self.assertRaises(HazelcastClientNotActiveError):
            self.manager.acquire_session(self.raft_group_id, 1).result()

    def test_acquire_session_with_unknown_group_id(self):
        m = self.mock_request_new_session()
        self.assertEqual(self.session_id, self.manager.acquire_session(self.raft_group_id, 3).result())
        self.assertEqual(3, self.get_acquire_count())
        m.assert_called_once_with(self.raft_group_id)

    def test_acquire_session_with_existing_invalid_session(self):
        m = self.mock_request_new_session()
        state = MagicMock(is_valid=lambda: False)
        self.set_session(state)

        self.assertEqual(self.session_id, self.manager.acquire_session(self.raft_group_id, 1).result())
        m.assert_called_once_with(self.raft_group_id)
        self.assertEqual(1, self.get_acquire_count())

    def test_acquire_session_for_valid_session(self):
        m = self.mock_request_new_session()
        self.set_session(self.prepare_state())

        self.assertEqual(self.session_id, self.manager.acquire_session(self.raft_group_id, 10).result())
        m.assert_not_called()
        self.assertEqual(10, self.get_acquire_count())

    def test_release_session(self):
        self.set_session(self.prepare_state())

        self.manager.release_session(self.raft_group_id, self.session_id, 3)
        self.assertEqual(-3, self.get_acquire_count())

    def test_release_session_with_unknown_session(self):
        self.set_session(self.prepare_state())

        self.manager.release_session(self.raft_group_id, -1, 3)
        self.assertEqual(0, self.get_acquire_count())

    def test_invalidate_session(self):
        self.set_session(self.prepare_state())

        self.manager.invalidate_session(self.raft_group_id, self.session_id)
        self.assertEqual(0, len(self.manager._sessions))

    def test_invalidate_session_with_unknown_session(self):
        self.set_session(self.prepare_state())

        self.manager.invalidate_session(self.raft_group_id, self.session_id - 1)
        self.assertEqual(1, len(self.manager._sessions))

    def test_create_thread_id_after_shutdown(self):
        self.manager.shutdown().result()

        with self.assertRaises(HazelcastClientNotActiveError):
            self.manager.get_or_create_unique_thread_id(self.raft_group_id).result()

    def test_create_thread_id(self):
        m = self.mock_request_generate_thread_id(5)
        self.assertEqual(5, self.manager.get_or_create_unique_thread_id(self.raft_group_id).result())
        m.assert_called_once_with(self.raft_group_id)
        self.assertEqual(5, self.manager._thread_ids.get((self.raft_group_id, thread_id())))

    def test_create_thread_id_with_known_group_id(self):
        m = self.mock_request_generate_thread_id(12)
        self.set_thread_id(13)
        self.assertEqual(13, self.manager.get_or_create_unique_thread_id(self.raft_group_id).result())
        m.assert_not_called()
        self.assertEqual(13, self.manager._thread_ids.get((self.raft_group_id, thread_id())))

    def test_shutdown(self):
        self.set_session(self.prepare_state())
        self.set_thread_id(123)
        self.manager._mutexes[self.raft_group_id] = object()
        m = MagicMock(return_value=ImmediateFuture(True))
        self.manager._request_close_session = m

        self.manager.shutdown().result()
        m.assert_called_once_with(self.raft_group_id, self.session_id)
        self.assertEqual(0, len(self.manager._sessions))
        self.assertEqual(0, len(self.manager._mutexes))
        self.assertEqual(0, len(self.manager._thread_ids))

    def test_heartbeat(self):
        reactor = self.mock_reactor()
        self.mock_request_new_session()

        r = MagicMock(return_value=ImmediateFuture(None))
        self.manager._request_heartbeat = r
        self.manager.acquire_session(self.raft_group_id, 1).result()
        time.sleep(2)
        self.manager.shutdown()
        reactor.shutdown()
        self.assertGreater(self.context.reactor.add_timer.call_count, 1)  # assert that the heartbeat task is executed
        r.assert_called()
        r.assert_called_with(self.raft_group_id, self.session_id)
        self.assertEqual(1, len(self.manager._sessions))

    def test_heartbeat_when_session_is_released(self):
        reactor = self.mock_reactor()
        self.mock_request_new_session()

        r = MagicMock(return_value=ImmediateFuture(None))
        self.manager._request_heartbeat = r
        self.manager.acquire_session(self.raft_group_id, 1).add_done_callback(
            lambda _: self.manager.release_session(self.raft_group_id, self.session_id, 1))
        time.sleep(2)
        self.manager.shutdown()
        reactor.shutdown()
        self.assertGreater(self.context.reactor.add_timer.call_count, 1)  # assert that the heartbeat task is executed
        r.assert_not_called()
        self.assertEqual(1, len(self.manager._sessions))

    def test_heartbeat_on_failure(self):
        reactor = self.mock_reactor()
        self.mock_request_new_session()
        self.manager._request_heartbeat = MagicMock(return_value=ImmediateExceptionFuture(SessionExpiredError()))

        m = MagicMock(side_effect=self.manager._invalidate_session)
        self.manager._invalidate_session = m

        self.manager.acquire_session(self.raft_group_id, 1).result()
        time.sleep(2)
        self.manager.shutdown()
        reactor.shutdown()
        self.assertGreater(self.context.reactor.add_timer.call_count, 1)  # assert that the heartbeat task is executed
        m.assert_called_once_with(self.raft_group_id, self.session_id)
        self.assertEqual(0, len(self.manager._sessions))

    def mock_request_generate_thread_id(self, t_id):
        def mock(*_, **__):
            return ImmediateFuture(t_id)

        m = MagicMock(side_effect=mock)
        self.manager._request_generate_thread_id = m
        return m

    def mock_request_new_session(self, ):
        def mock(*_, **__):
            d = {
                "session_id": self.session_id,
                "ttl_millis": 1000,
                "heartbeat_millis": 100,
            }
            return ImmediateFuture(d)

        m = MagicMock(side_effect=mock)
        self.manager._request_new_session = m
        return m

    def prepare_state(self):
        return _SessionState(self.session_id, self.raft_group_id, 1)

    def get_acquire_count(self):
        return self.manager._sessions[self.raft_group_id].acquire_count.get()

    def set_session(self, state):
        self.manager._sessions[self.raft_group_id] = state

    def set_thread_id(self, global_t_id):
        self.manager._thread_ids[(self.raft_group_id, thread_id())] = global_t_id

    def mock_reactor(self):
        r = AsyncoreReactor({})
        r.start()
        m = MagicMock()
        m.add_timer = MagicMock(side_effect=lambda d, c: r.add_timer(d, c))
        self.context.reactor = m
        return r
