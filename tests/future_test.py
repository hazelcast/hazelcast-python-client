import sys
import traceback
import unittest
from threading import Thread, Event

from hazelcast.future import Future, ImmediateFuture, combine_futures, make_blocking, ImmediateExceptionFuture


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
        exception = f.exception()
        traceback = f.traceback()
        exc = exc[0]
        self.assertEqual(exc[1], exception)
        self.assertEqual(exc[2], traceback)

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

    def test_callback_called_exactly_once(self):
        for _ in xrange(0, 10000):
            f = Future()

            def set_result():
                f.set_result("done")

            t = Thread(target=set_result)
            t.start()

            i = [0]

            def callback(c):
                i[0] += 1

            f.add_done_callback(callback)
            t.join()
            self.assertEqual(i[0], 1)

    def test_callback_called_exactly_once_when_exception(self):
        for _ in xrange(0, 10000):
            f = Future()

            def set_exception():
                f.set_exception(RuntimeError("error"))

            t = Thread(target=set_exception)
            t.start()

            i = [0]

            def callback(c):
                i[0] += 1

            f.add_done_callback(callback)
            t.join()
            self.assertEqual(i[0], 1)

    def test_done(self):
        f = Future()

        self.assertFalse(f.done())

        f.set_result("done")

        self.assertTrue(f.done())

    def test_running(self):
        f = Future()

        self.assertTrue(f.running())

        f.set_result("done")

        self.assertFalse(f.running())

    def test_set_exception_with_non_exception(self):
        f = Future()
        with self.assertRaises(RuntimeError):
            f.set_exception("non-exception")

    def test_callback_throws_exception(self):
        f = Future()

        def invalid_func():
            raise RuntimeError("error!")

        f.add_done_callback(invalid_func)
        f.set_result("done")  # should not throw exception

    def test_continue_with_throws_exception(self):
        f = Future()
        e = RuntimeError("error")

        def continue_func(f):
            raise e

        n = f.continue_with(continue_func)
        f.set_result("done")

        self.assertFalse(n.is_success())
        self.assertEqual(n.exception(), e)


class ImmediateFutureTest(unittest.TestCase):
    f = None

    def setUp(self):
        self.f = ImmediateFuture("done")

    def test_result(self):
        self.assertEqual("done", self.f.result())

    def test_exception(self):
        self.assertIsNone(self.f.exception())

    def test_traceback(self):
        self.assertIsNone(self.f.traceback())

    def test_set_result(self):
        with self.assertRaises(NotImplementedError):
            self.f.set_result("done")

    def test_set_exception(self):
        with self.assertRaises(NotImplementedError):
            self.f.set_exception(RuntimeError())

    def test_is_succcess(self):
        self.assertTrue(self.f.is_success())

    def test_is_done(self):
        self.assertTrue(self.f.done())

    def test_is_not_running(self):
        self.assertFalse(self.f.running())

    def test_callback(self):
        n = [0]

        def callback(f):
            self.assertEqual(f, self.f)
            n[0] += 1

        self.f.add_done_callback(callback)

        self.assertEqual(n[0], 1)


class ImmediateExceptionFutureTest(unittest.TestCase):
    f = None

    def setUp(self):
        try:
            raise RuntimeError("error")
        except:
            self.exc = sys.exc_info()[1]
            self.traceback = sys.exc_info()[2]
            self.f = ImmediateExceptionFuture(self.exc, self.traceback)

    def test_result(self):
        with self.assertRaises(type(self.exc)):
            self.f.result()

    def test_exception(self):
        self.assertEqual(self.exc, self.f.exception())

    def test_traceback(self):
        self.assertEqual(self.traceback, self.f.traceback())

    def test_set_result(self):
        with self.assertRaises(NotImplementedError):
            self.f.set_result("done")

    def test_set_exception(self):
        with self.assertRaises(NotImplementedError):
            self.f.set_exception(RuntimeError())

    def test_is_succcess(self):
        self.assertFalse(self.f.is_success())

    def test_is_done(self):
        self.assertTrue(self.f.done())

    def test_is_not_running(self):
        self.assertFalse(self.f.running())

    def test_callback(self):
        n = [0]

        def callback(f):
            self.assertEqual(f, self.f)
            n[0] += 1

        self.f.add_done_callback(callback)

        self.assertEqual(n[0], 1)


class CombineFutureTest(unittest.TestCase):
    def test_combine_futures(self):
        f1, f2, f3 = Future(), Future(), Future()

        combined = combine_futures(f1, f2, f3)

        f1.set_result("done1")
        self.assertFalse(combined.done())

        f2.set_result("done2")
        self.assertFalse(combined.done())

        f3.set_result("done3")
        self.assertEqual(combined.result(), ["done1", "done2", "done3"])

    def test_combine_futures_exception(self):
        f1, f2, f3 = Future(), Future(), Future()

        combined = combine_futures(f1, f2, f3)

        e = RuntimeError("error")
        f1.set_result("done")
        f2.set_result("done")
        f3.set_exception(e)

        self.assertEqual(e, combined.exception())


class MakeBlockingTest(unittest.TestCase):
    class Calculator(object):
        def __init__(self):
            self.name = "calc"

        def add_one(self, x):
            f = Future()
            f.set_result(x + 1)
            return f

        def multiply(self, x, y):
            f = Future()
            f.set_result(x * y)
            return f

        def multiply_sync(self, x, y):
            return x * y

    def setUp(self):
        self.calculator = make_blocking(MakeBlockingTest.Calculator())

    def test_args(self):
        self.assertEqual(self.calculator.add_one(1), 2)

    def test_kwargs(self):
        self.assertEqual(self.calculator.multiply(x=4, y=5), 20)

    def test_blocking_method(self):
        self.assertEqual(self.calculator.multiply_sync(x=4, y=5), 20)

    def test_missing_method(self):
        with self.assertRaises(AttributeError):
            self.calculator.missing_method()

    def test_attribute(self):
        self.assertEqual(self.calculator.name, "calc")
