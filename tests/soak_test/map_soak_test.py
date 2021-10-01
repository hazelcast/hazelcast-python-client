import argparse
import logging
import os
import random
import sys
import threading
import time
from collections import defaultdict

sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))
# Making sure that hazelcast directory is in the sys.path so we can import modules from there in the command line.

from hazelcast.client import HazelcastClient
from hazelcast.predicate import between
from hazelcast.serialization.api import IdentifiedDataSerializable

if hasattr(time, "monotonic"):
    get_current_timestamp = time.monotonic
else:
    get_current_timestamp = time.time


THREAD_COUNT = 32
ENTRY_COUNT = 10000
OBSERVATION_INTERVAL = 10.0


class SoakTestCoordinator(object):
    def __init__(self, test_duration, address):
        self._tests_failed = False
        self._thread_count_before = threading.active_count()
        self._deadline = get_current_timestamp() + test_duration * 60 * 60
        self._reached_deadline = False
        self._client = self._init_client(address)
        self._test_map = self._init_test_map()
        self._test_runners = self._init_test_runners()
        self._observer = OperationCountObserver(self)

    @property
    def test_map(self):
        return self._test_map

    @property
    def test_runners(self):
        return self._test_runners

    def start_tests(self):
        test_runners = self._test_runners
        observer = self._observer

        logging.info("Soak test operations are starting!")
        logging.info("* " * 20 + "\n")

        for test_runner in test_runners:
            test_runner.start()

        observer.start()
        observer.join()

        for test_runner in test_runners:
            test_runner.join()

        self._client.shutdown()

        logging.info("* " * 20)
        logging.info("Soak test has finished!")
        logging.info("-" * 40)

        if self._tests_failed:
            logging.info("Soak test failed!")
            return

        hang_counts = observer.hang_counts
        if not hang_counts:
            logging.info("All threads worked without hanging")
        else:
            for test_runner, hang_count in hang_counts:
                logging.info("Thread %s hanged %s times.", test_runner, hang_count)

        thread_count_after = threading.active_count()
        logging.info(
            "Thread count before: %s, after: %s", self._thread_count_before, thread_count_after
        )

    def notify_error(self):
        self._tests_failed = True

    def should_continue_tests(self):
        return not (self._reached_deadline or self._tests_failed)

    def check_deadline(self):
        now = get_current_timestamp()
        self._reached_deadline = now >= self._deadline

    @staticmethod
    def _init_client(address):
        try:
            return HazelcastClient(
                cluster_members=[address],
                data_serializable_factories={
                    SimpleEntryProcessor.FACTORY_ID: {
                        SimpleEntryProcessor.CLASS_ID: SimpleEntryProcessor
                    }
                },
            )
        except Exception as e:
            logging.exception("Client failed to start")
            raise e

    def _init_test_map(self):
        def no_op_listener(_):
            pass

        test_map = self._client.get_map("test-map").blocking()
        test_map.add_entry_listener(
            include_value=False,
            added_func=no_op_listener,
            removed_func=no_op_listener,
            updated_func=no_op_listener,
        )
        return test_map

    def _init_test_runners(self):
        test_runners = []

        for _ in range(THREAD_COUNT):
            counter = OperationCounter()
            test_runner = TestRunner(self, counter)
            test_runners.append(test_runner)

        return test_runners


class TestRunner(threading.Thread):
    def __init__(self, coordinator, counter, *args, **kwargs):
        super(TestRunner, self).__init__(*args, **kwargs)
        self._coordinator = coordinator
        self._counter = counter

    @property
    def counter(self):
        return self._counter

    def run(self):
        coordinator = self._coordinator
        counter = self._counter
        test_map = coordinator.test_map
        processor = SimpleEntryProcessor("test")

        while coordinator.should_continue_tests():
            key = str(random.randint(0, ENTRY_COUNT))
            value = str(random.randint(0, ENTRY_COUNT))
            operation = random.randint(0, 100)
            try:
                if operation < 30:
                    test_map.get(key)
                elif operation < 60:
                    test_map.put(key, value)
                elif operation < 80:
                    test_map.values(between("this", 0, 10))
                else:
                    test_map.execute_on_key(key, processor)

                counter.increment()
            except:
                coordinator.notify_error()
                logging.exception("Unexpected error occurred in thread %s", self)
                return


class OperationCountObserver(threading.Thread):
    def __init__(self, coordinator, *args, **kwargs):
        super(OperationCountObserver, self).__init__(*args, **kwargs)
        self._coordinator = coordinator
        self._hang_counts = defaultdict(int)

    @property
    def hang_counts(self):
        return self._hang_counts

    def run(self):
        coordinator = self._coordinator
        test_runners = coordinator.test_runners
        hang_counts = self._hang_counts

        while True:
            time.sleep(OBSERVATION_INTERVAL)

            coordinator.check_deadline()
            if not coordinator.should_continue_tests():
                break

            logging.info("-" * 40)

            op_count = 0
            hanged_runners = []

            for test_runner in test_runners:
                op_count_per_runner = test_runner.counter.get_and_reset()
                op_count += op_count_per_runner
                if op_count == 0:
                    hanged_runners.append(test_runner)

            if not hanged_runners:
                logging.info("All threads worked without hanging")
            else:
                logging.info("%s threads hanged: %s", len(hanged_runners), hanged_runners)
                for hanged_runner in hanged_runners:
                    hang_counts[hanged_runner] += 1

            logging.info("-" * 40)
            logging.info("Operations Per Second: %s\n", op_count / OBSERVATION_INTERVAL)


class OperationCounter(object):
    def __init__(self):
        self._count = 0
        self._lock = threading.Lock()

    def get_and_reset(self):
        with self._lock:
            total = self._count
            self._count = 0
            return total

    def increment(self):
        with self._lock:
            self._count += 1


class SimpleEntryProcessor(IdentifiedDataSerializable):
    CLASS_ID = 1
    FACTORY_ID = 66

    def __init__(self, value):
        self.value = value

    def read_data(self, object_data_input):
        pass

    def write_data(self, object_data_output):
        object_data_output.write_string(self.value)

    def get_class_id(self):
        return self.CLASS_ID

    def get_factory_id(self):
        return self.FACTORY_ID


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--duration",
        default=48.0,
        type=float,
        help="Duration of the test in hours",
    )
    parser.add_argument(
        "--address",
        required=True,
        type=str,
        help="host:port of the one of the cluster members",
    )
    parser.add_argument(
        "--log-file",
        required=True,
        type=str,
        help="Name of the log file",
    )
    return parser.parse_args()


def setup_logging(log_file):
    logging.basicConfig(
        filename=log_file,
        filemode="w",
        format="%(asctime)s %(message)s",
        datefmt="%H:%M:%S",
        level=logging.INFO,
    )


def start_coordinator(duration, address):
    coordinator = SoakTestCoordinator(duration, address)
    coordinator.start_tests()


if __name__ == "__main__":
    arguments = parse_arguments()
    setup_logging(arguments.log_file)
    start_coordinator(arguments.duration, arguments.address)
