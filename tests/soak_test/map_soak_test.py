import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))
# Making sure that hazelcast directory is in the sys.path so we can import modules from there in the command line.

import argparse
import threading
import logging
import time
import random

from hazelcast.client import HazelcastClient
from hazelcast.serialization.api import IdentifiedDataSerializable
from hazelcast.predicate import between

THREAD_COUNT = 32
ENTRY_COUNT = 10000
STATS_DISPLAY_SECONDS = 10.0


class Statistic(object):
    def __init__(self, thread_id):
        self.id = thread_id
        self._total_ops = 0
        self._lock = threading.Lock()

    def get_total_operation_count_and_reset(self):
        with self._lock:
            total = self._total_ops
            self._total_ops = 0
            return total

    def increment_operation_count(self):
        with self._lock:
            self._total_ops += 1


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


def listener(_):
    pass


def start():
    thread_count_before = threading.active_count()
    parser = argparse.ArgumentParser()
    parser.add_argument("--hour", default=48.0, type=float, help="Duration of the test in hours")
    parser.add_argument(
        "--addresses",
        default="127.0.0.1",
        type=str,
        help="List of cluster member addresses separated by -",
    )
    parser.add_argument("--log", default="default_log_file", type=str, help="Name of the log file")

    args = parser.parse_args()

    hour_limit = args.hour
    addresses = args.addresses
    log_file = args.log

    logging.basicConfig(
        filename=log_file,
        filemode="w",
        format="%(asctime)s %(message)s",
        datefmt="%H:%M:%S",
        level=logging.INFO,
    )

    try:
        client = HazelcastClient(
            cluster_members=addresses.split("-"),
            data_serializable_factories={
                SimpleEntryProcessor.FACTORY_ID: {
                    SimpleEntryProcessor.CLASS_ID: SimpleEntryProcessor
                }
            },
        )
    except Exception:
        logging.exception("Client failed to start")
        return

    processor = SimpleEntryProcessor("test")
    test_map = client.get_map("test-map").blocking()
    test_map.add_entry_listener(
        False, added_func=listener, removed_func=listener, updated_func=listener
    )

    logging.info("Soak test operations are starting!")
    logging.info("* " * 20 + "\n")

    test_failed = [False]

    def run(stats):
        end_time = time.time() + hour_limit * 60 * 60
        while time.time() < end_time:
            if test_failed[0]:
                return  # Some other thread failed, no need to continue the test

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

                stats.increment_operation_count()
            except:
                test_failed[0] = True
                logging.exception("Unexpected error occurred")
                return

    statistics = []
    threads = []
    thread_hang_counts = {}

    for i in range(THREAD_COUNT):
        statistic = Statistic(i)
        statistics.append(statistic)
        thread = threading.Thread(target=run, args=(statistic,))
        threads.append(thread)
        thread_hang_counts[i] = 0
        thread.start()

    def display_statistics():
        end_time = time.time() + hour_limit * 60 * 60
        while time.time() < end_time:
            time.sleep(STATS_DISPLAY_SECONDS)
            if test_failed[0]:
                # Some thread failed. No need to continue.
                return

            logging.info("-" * 40)

            total_ops = 0  # sum of total ops of all threads
            hanged_threads = []
            for stat in statistics:
                total = stat.get_total_operation_count_and_reset()  # per thread
                total_ops += total
                if total == 0:
                    hanged_threads.append(stat.id)

            if not hanged_threads:
                logging.info("All threads worked without hanging")
            else:
                logging.info("%s threads hanged with ids %s", len(hanged_threads), hanged_threads)
                for thread_id in hanged_threads:
                    thread_hang_counts[thread_id] += 1

            logging.info("-" * 40)
            logging.info("Operations Per Second: %s\n", total_ops / STATS_DISPLAY_SECONDS)

    display_thread = threading.Thread(target=display_statistics)
    display_thread.start()
    display_thread.join()

    for thread in threads:
        thread.join()

    client.shutdown()

    logging.info("* " * 20)
    logging.info("Soak test has finished!")
    logging.info("-" * 40)

    if test_failed[0]:
        logging.info("Soak test failed!")
        return

    if sum(thread_hang_counts.values()) == 0:
        logging.info("All threads worked without hanging")
    else:
        for key in thread_hang_counts:
            hang_count = thread_hang_counts[key]
            if hang_count != 0:
                logging.info("Thread %s hanged %s times.", key, hang_count)

    thread_count_after = threading.active_count()
    logging.info("Thread count before: %s, after: %s", thread_count_before, thread_count_after)


if __name__ == "__main__":
    start()
