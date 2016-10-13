import logging
import sys
import threading

from hazelcast.util import AtomicInteger

NONE_RESULT = object()


class Future(object):
    """
    Future is used for representing an asynchronous computation result.
    """
    _result = None
    _exception = None
    _traceback = None
    _threading_locals = threading.local()
    logger = logging.getLogger("Future")

    def __init__(self):
        self._callbacks = []
        self._event = _Event()

    def set_result(self, result):
        """
        Sets the result of the Future.

        :param result: Result of the Future.
        """
        if result is None:
            self._result = NONE_RESULT
        else:
            self._result = result
        self._event.set()
        self._invoke_callbacks()

    def set_exception(self, exception, traceback=None):
        """
        Sets the exception for this Future in case of errors.

        :param exception: (Exception), exception to be threw in case of error.
        :param traceback: (Function), function to be called on traceback (optional).
        """
        if not isinstance(exception, BaseException):
            raise RuntimeError("Exception must be of BaseException type")
        self._exception = exception
        self._traceback = traceback
        self._event.set()
        self._invoke_callbacks()

    def result(self):
        """
        Returns the result of the Future, which makes the call synchronous if the result has not been computed yet.

        :return: Result of the Future.
        """
        self._reactor_check()
        self._event.wait()
        if self._exception:
            raise self._exception, None, self._traceback
        if self._result == NONE_RESULT:
            return None
        else:
            return self._result

    def _reactor_check(self):
        if not self.done() and hasattr(self._threading_locals, 'is_reactor_thread'):
            raise RuntimeError(
                    "Synchronous result for incomplete operation must not be called from Reactor thread. "
                    "Use add_done_callback instead.")

    def is_success(self):
        """
        Determines whether the result can be successfully computed or not.
        """
        return self._result is not None

    def done(self):
        """
        Determines whether the result is computed or not.

        :return: (bool), ``true`` if the result is computed, ``false`` otherwise.
        """
        return self._event.is_set()

    def running(self):
        """
        Determines whether the asynchronous call, the computation is still running or not.

        :return: (bool), ``true`` if the  result is being computed, ``false`` otherwise.
        """
        return not self.done()

    def exception(self):
        """
        Throws exception.
        :return: (Exception), exception of this Future.
        """
        self._reactor_check()
        self._event.wait()
        return self._exception

    def traceback(self):
        """
        Traceback function for the Future.
        """
        self._reactor_check()
        self._event.wait()
        return self._traceback

    def add_done_callback(self, callback):
        run_callback = False
        with self._event.condition:
            if self.done():
                run_callback = True
            else:
                self._callbacks.append(callback)

        if run_callback:
            self._invoke_cb(callback)

    def _invoke_callbacks(self):
        for callback in self._callbacks:
            self._invoke_cb(callback)

    def _invoke_cb(self, callback):
        try:
            callback(self)
        except:
            logging.exception("Exception when invoking callback")

    def continue_with(self, continuation_func, *args):
        """
        Create a continuation that executes when the Future is completed.

        :param continuation_func: A function which takes the future as the only parameter. Return value of the function
        will be set as the result of the continuation future.
        :return: A new Future which will be completed when the continuation is done.
        """
        future = Future()

        def callback(f):
            try:
                future.set_result(continuation_func(f, *args))
            except:
                future.set_exception(sys.exc_info()[1], sys.exc_info()[2])

        self.add_done_callback(callback)
        return future


class _Event(object):
    _flag = False

    def __init__(self):
        self.condition = threading.Condition(threading.Lock())

    def set(self):
        with self.condition:
            self._flag = True
            self.condition.notify_all()

    def is_set(self):
        return self._flag

    def wait(self):
        with self.condition:
            if not self._flag:
                self.condition.wait()
            return self._flag


class ImmediateFuture(Future):
    def __init__(self, result):
        self._result = result

    def set_exception(self, exception):
        raise NotImplementedError()

    def set_result(self, result):
        raise NotImplementedError()

    def done(self):
        return True

    def is_success(self):
        return True

    def exception(self):
        return None

    def traceback(self):
        return None

    def result(self):
        return self._result

    def add_done_callback(self, callback):
        self._invoke_cb(callback)


class ImmediateExceptionFuture(Future):
    def __init__(self, exception, traceback=None):
        self._exception = exception
        self._traceback = traceback

    def set_exception(self, exception, traceback=None):
        raise NotImplementedError()

    def set_result(self, result):
        raise NotImplementedError()

    def done(self):
        return True

    def is_success(self):
        return False

    def exception(self):
        return self._exception

    def traceback(self):
        return self._traceback

    def result(self):
        raise self._exception, None, self._traceback

    def add_done_callback(self, callback):
        self._invoke_cb(callback)


def combine_futures(*futures):
    """
    Combines set of Futures.

    :param futures: (Futures), Futures to be combined.
    :return: Result of the combination.
    """
    expected = len(futures)
    results = []
    completed = AtomicInteger()
    combined = Future()

    def done(f):
        if not combined.done():
            if f.is_success():  # TODO: ensure ordering of results as original list
                results.append(f.result())
                if completed.get_and_increment() + 1 == expected:
                    combined.set_result(results)
            else:
                combined.set_exception(f.exception(), f.traceback())

    for future in futures:
        future.add_done_callback(done)

    return combined


class _BlockingWrapper(object):
    def __init__(self, wrapped):
        self._wrapped = wrapped

    def __getattr__(self, item):
        inner = getattr(self._wrapped, item)
        if callable(inner):
            return self.wrap(inner)
        return inner

    def wrap(self, inner):
        def f(*args, **kwargs):
            result = inner(*args, **kwargs)
            if isinstance(result, Future):
                return result.result()
            return result

        return f

    def __repr__(self):
        return self._wrapped.__repr__()


def make_blocking(instance):
    """
    Takes an instance and returns an object whose methods which return non-blocking Future become blocking calls.
    :param instance: (object), a non-blocking instance.
    :return: (object), blocking version of given non-blocking instance.
    """
    return _BlockingWrapper(instance)
