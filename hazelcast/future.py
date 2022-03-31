import logging
import sys
import threading
import types

import typing

from hazelcast.util import AtomicInteger, re_raise

_logger = logging.getLogger(__name__)
_SENTINEL = object()

ResultType = typing.TypeVar("ResultType")


class Future(typing.Generic[ResultType]):
    """Future is used for representing an asynchronous computation result."""

    _result = _SENTINEL
    _exception = None
    _traceback = None
    _threading_locals = threading.local()

    def __init__(self):
        self._callbacks = []
        self._event = _Event()

    def set_result(self, result: ResultType) -> None:
        """Sets the result of the Future.

        Args:
            result: Result of the Future.
        """
        self._result = result
        self._event.set()
        self._invoke_callbacks()

    def set_exception(self, exception: Exception, traceback: types.TracebackType = None) -> None:
        """Sets the exception for this Future in case of errors.

        Args:
            exception: Exception to raise in case of error.
            traceback: Traceback of the exception.
        """
        if not isinstance(exception, BaseException):
            raise RuntimeError("Exception must be of BaseException type")

        self._exception = exception
        self._traceback = traceback
        self._event.set()
        self._invoke_callbacks()

    def result(self) -> ResultType:
        """Returns the result of the Future, which makes the call synchronous
        if the result has not been computed yet.

        Returns:
            Result of the Future.
        """
        self._reactor_check()
        self._event.wait()
        if self._exception:
            re_raise(self._exception, self._traceback)

        # Result will be set to the correct type before we
        # return from here
        return self._result  # type: ignore[return-value]

    def _reactor_check(self):
        if not self.done() and hasattr(self._threading_locals, "is_reactor_thread"):
            raise RuntimeError(
                "Synchronous result for incomplete operation must not be called from Reactor thread. "
                "Use add_done_callback instead."
            )

    def is_success(self) -> bool:
        """Determines whether the result can be successfully computed or not."""
        return self._result is not _SENTINEL

    def done(self) -> bool:
        """Determines whether the result is computed or not.

        Returns:
            ``True`` if the result is computed, ``False`` otherwise.
        """
        return self._event.is_set()

    def running(self) -> bool:
        """Determines whether the asynchronous call, the computation is still
        running or not.

        Returns:
            ``True`` if the  result is being computed, ``False`` otherwise.
        """
        return not self.done()

    def exception(self) -> typing.Optional[Exception]:
        """Returns the exceptional result, if any.

        Returns:
            Exceptional result of this Future.
        """
        self._reactor_check()
        self._event.wait()
        return self._exception

    def traceback(self) -> typing.Optional[types.TracebackType]:
        """Traceback of the exception."""
        self._reactor_check()
        self._event.wait()
        return self._traceback

    def add_done_callback(self, callback: typing.Callable[["Future"], None]) -> None:
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
            _logger.exception("Exception when invoking callback")

    def continue_with(
        self, continuation_func: typing.Callable[..., typing.Any], *args: typing.Any
    ) -> "Future":
        """Create a continuation that executes when the Future is completed.

        Args:
            continuation_func: A function which takes the Future as the only
                parameter. Return value of the function will be set as the
                result of the continuation future. If the return value of the
                function is another Future, it will be chained to the returned
                Future.
            *args: Arguments to be passed into ``continuation_function``.

        Returns:
            A new Future which will be completed when the continuation is done.
        """
        future: Future[typing.Any] = Future()

        def callback(f):
            try:
                result = continuation_func(f, *args)
                if isinstance(result, Future):
                    future._chain(result)
                else:
                    future.set_result(result)
            except:
                exception, traceback = sys.exc_info()[1:]
                future.set_exception(exception, traceback)

        self.add_done_callback(callback)
        return future

    def _chain(self, chained_future):
        def callback(f):
            try:
                result = f.result()
                if isinstance(result, Future):
                    self._chain(result)
                else:
                    self.set_result(result)
            except:
                exception, traceback = sys.exc_info()[1:]
                self.set_exception(exception, traceback)

        chained_future.add_done_callback(callback)


class _Event:
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

    def set_exception(self, exception, traceback=None):
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
        re_raise(self._exception, self._traceback)

    def add_done_callback(self, callback):
        self._invoke_cb(callback)


def combine_futures(futures: typing.Sequence[Future]) -> Future:
    """Combines set of Futures.

    It waits for the completion of the all input Futures regardless
    of their output.

    The returned Future completes with the list of the results of the input
    Futures, respecting the input order.

    If one of the input Futures completes exceptionally, the returned
    Future also completes exceptionally. In case of multiple exceptional
    completions, the returned Future will be completed with the first
    exceptional result.

    Args:
        futures: List of Futures to be combined.

    Returns:
        Result of the combination.
    """
    count = len(futures)
    results = [None] * count
    if count == 0:
        return ImmediateFuture(results)

    completed = AtomicInteger()
    combined: Future[typing.List[typing.Any]] = Future()
    errors: typing.List[typing.Tuple[Exception, types.TracebackType]] = []

    def done(future, index):
        if future.is_success():
            results[index] = future.result()
        else:
            if not errors:
                # We are fine with this check-then-act.
                # At most, we will end up with couple of
                # errors stored in case of the concurrent calls.
                # The idea behind this check is to try to minimize
                # the number of errors we store without
                # synchronization, as we only need the first error.
                errors.append((future.exception(), future.traceback()))

        if count == completed.increment_and_get():
            if errors:
                first_exception, first_traceback = errors[0]
                combined.set_exception(first_exception, first_traceback)
            else:
                combined.set_result(results)

    def make_callback(index):
        return lambda f: done(f, index)

    for i, future in enumerate(futures):
        future.add_done_callback(make_callback(i))

    return combined
