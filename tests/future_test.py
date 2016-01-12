import sys
import traceback
import unittest
from threading import Thread, Event

from hazelcast.future import Future


class FutureTest(unittest.TestCase):
    def test_set_result(self):
        f = Future()

        def set_result():
            f.set_result("done")

        Thread(target=set_result).start()
        self.assertEqual(f.result(), "done")

    def test_set_exception(self):
        f = Future()
        exc = []

        def set_exception():
            try:
                {}["invalid_key"]
            except KeyError as e:
                exc.append(sys.exc_info())
                f.set_exception(e, sys.exc_info()[2])

        Thread(target=set_exception).start()
        exc = exc[0]
        self.assertEqual(exc[1], f.exception())
        self.assertEqual(exc[2], f.traceback())

    def test_result_raises_exception_with_traceback(self):
        f = Future()
        exc_info = None
        try:
            {}["invalid_key"]
        except KeyError as e:
            exc_info = sys.exc_info()
            f.set_exception(e, sys.exc_info()[2])

        try:
            f.result()
            self.fail("Future.result() should raise exception")
        except:
            info = sys.exc_info()
            self.assertEqual(info[1], exc_info[1])

            original_tb = traceback.extract_tb(exc_info[2])
            # shift traceback by one to discard the last frame
            actual_tb = traceback.extract_tb(info[2])[1:]

            self.assertEqual(original_tb, actual_tb)

    def test_add_callback_with_success(self):
        f = Future()
        e = Event()

        def set_result():
            f.set_result("done")

        def callback(future):
            self.assertEqual(future.result(), "done")
            e.set()

        f.add_done_callback(callback)

        Thread(target=set_result).start()

        self.assertTrue(e.wait(timeout=5))

    def test_add_callback_with_failure(self):
        f = Future()
        e = Event()
        exc = []

        def set_exception():
            map = {}
            try:
                a = map["invalid_key"]
            except KeyError as e:
                exc.append(sys.exc_info())
                f.set_exception(e, sys.exc_info()[2])

        def callback(future):
            exc_info = exc[0]
            self.assertEqual(exc_info[1], future.exception())
            self.assertEqual(exc_info[2], future.traceback())
            e.set()

        f.add_done_callback(callback)

        Thread(target=set_exception).start()

        self.assertTrue(e.wait(timeout=5))

    def test_add_callback_after_completion_with_success(self):
        f = Future()
        f.set_result("done")

        counter = [0]

        def callback(future):
            counter[0] += 1
            self.assertEqual(future.result(), "done")

        f.add_done_callback(callback)
        self.assertEqual(counter[0], 1)

    def test_add_callback_after_completion_with_error(self):
        f = Future()
        error = RuntimeError("error")
        f.set_exception(error, None)

        counter = [0]

        def callback(future):
            counter[0] += 1
            self.assertEqual(future.exception(), error)

        f.add_done_callback(callback)
        self.assertEqual(counter[0], 1)

    def test_get_result_from_reactor_thread(self):
        f = Future()
        e = Event()

        def get_result():
            f._threading_locals.is_reactor_thread = True
            try:
                f.result()
            except RuntimeError:
                e.set()

        Thread(target=get_result).start()
        self.assertTrue(e.wait(5), "event was not set")

    def test_get_exception_from_reactor_thread(self):
        f = Future()
        e = Event()

        def get_result():
            f._threading_locals.is_reactor_thread = True
            try:
                f.exception()
            except RuntimeError:
                e.set()

        Thread(target=get_result).start()
        self.assertTrue(e.wait(5), "event was not set")

    def test_continue_with_on_success(self):
        f = Future()
        f.set_result(1)

        def continuation(future):
            return future.result() + 1

        result = f.continue_with(continuation).result()
        self.assertEqual(2, result)

    def test_continue_with_on_failure(self):
        f = Future()
        f.set_exception(RuntimeError("error"))

        def continuation(future):
            if future.is_success():
                return 0
            else:
                return 1

        result = f.continue_with(continuation).result()
        self.assertEqual(1, result)
