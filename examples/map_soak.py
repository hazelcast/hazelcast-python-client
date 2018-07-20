import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
# Making sure that hazelcast directory is in the sys.path so we can import modules from there in the command line.

import argparse
import threading
import logging
import time
import random

from hazelcast.client import HazelcastClient
from hazelcast.config import ClientConfig
from hazelcast.serialization.api import IdentifiedDataSerializable
from hazelcast.serialization.predicate import BetweenPredicate

THREAD_COUNT_CONST = 32
ENTRY_COUNT_CONST = 10000
FACTORY_ID_CONST = 66
TEST_VALUE_CONST = "test"
STATS_DISPLAY_SECONDS_CONST = 10
THREAD_LOCK = threading.Lock()


class Statistic(object):
    def __init__(self,
                 id,
                 exception_count=0,
                 get_count=0,
                 put_count=0,
                 values_count=0,
                 execute_on_key_count=0):
        self.id = id
        self.exception_count = exception_count
        self.get_count = get_count
        self.put_count = put_count
        self.values_count = values_count
        self.execute_on_key_count = execute_on_key_count

    def get_and_reset(self):
        with THREAD_LOCK:
            statistic = (self.id,
                         self.exception_count,
                         self.get_count,
                         self.put_count,
                         self.values_count,
                         self.execute_on_key_count)
            self.exception_count = 0
            self.get_count = 0
            self.put_count = 0
            self.values_count = 0
            self.execute_on_key_count = 0
            return Statistic(*statistic)

    def get_total_operation_count(self):
        with THREAD_LOCK:
            return self.get_count + self.put_count + self.values_count + self.execute_on_key_count

    def increment_exception_count(self):
        with THREAD_LOCK:
            self.exception_count += 1

    def increment_get_count(self):
        with THREAD_LOCK:
            self.get_count += 1

    def increment_put_count(self):
        with THREAD_LOCK:
            self.put_count += 1

    def increment_values_count(self):
        with THREAD_LOCK:
            self.values_count += 1

    def increment_execute_on_key_count(self):
        with THREAD_LOCK:
            self.execute_on_key_count += 1

    def __str__(self):
        return "Statistic[{} - Total= {} ]".format(self.id, self.get_total_operation_count())


class SimpleEntryProcessor(IdentifiedDataSerializable):
    CLASS_ID = 1
    FACTORY_ID = FACTORY_ID_CONST

    def __init__(self, value=TEST_VALUE_CONST):
        self.value = value

    def read_data(self, object_data_input):
        self.value = object_data_input.read_utf()

    def write_data(self, object_data_output):
        object_data_output.write_utf(self.value)

    def get_class_id(self):
        return self.CLASS_ID

    def get_factory_id(self):
        return self.FACTORY_ID


def listener(event):
    event.key
    event.value
    event.old_value


def start_map_soak_test():
    thread_count_before = threading.active_count()
    parser = argparse.ArgumentParser()
    parser.add_argument("--hour", default=0, type=float, help="a float for the time limit of soak test")
    parser.add_argument("--addresses", default="127.0.0.1", type=str, help="list of addresses separated by -")
    parser.add_argument("--log", default="default_log_file", type=str, help="name of the log file")

    args = parser.parse_args()

    hour_limit = args.hour
    addresses = args.addresses
    log_file = args.log

    logging.basicConfig(filename=log_file,
                        filemode="a",
                        format="%(asctime)s %(message)s",
                        datefmt="%H:%M:%S",
                        level=logging.INFO)

    config = ClientConfig()
    processor = SimpleEntryProcessor()
    processor_factory = {SimpleEntryProcessor.CLASS_ID: SimpleEntryProcessor}
    config.serialization_config.add_data_serializable_factory(SimpleEntryProcessor.FACTORY_ID, processor_factory)
    for address in addresses.split("-"):
        config.network_config.addresses.append(address)

    try:
        client = HazelcastClient(config)
    except Exception as ex:
        logging.info(ex.args[0])
        return

    try:
        test_map = client.get_map("test-map")
    except Exception as ex:
        logging.info(ex.args[0])
        return

    test_map.add_entry_listener(False, added_func=listener, removed_func=listener, updated_func=listener)

    logging.info("Soak test operations are starting!")
    logging.info("* " * 20 + "\n")

    def soak_test_function(statistic):
        end_time = time.time() + hour_limit * 60 * 60
        while time.time() < end_time:
            key = str(random.randint(0, ENTRY_COUNT_CONST))
            value = str(random.randint(0, ENTRY_COUNT_CONST))
            operation = random.randint(0, 100)
            if operation < 30:
                try:
                    test_map.get(key).result()
                    statistic.increment_get_count()
                except Exception as e:
                    logging.info("Error in get() : " + e.args[0])
                    statistic.increment_exception_count()
            elif operation < 60:
                try:
                    test_map.put(key, value).result()
                    statistic.increment_put_count()
                except Exception as e:
                    logging.info("Error in put() : " + e.args[0])
                    statistic.increment_exception_count()
            elif operation < 80:
                try:
                    test_map.values(BetweenPredicate("this", 0, 10)).result()
                    statistic.increment_values_count()
                except Exception as e:
                    logging.info("Error in values() : " + e.args[0])
                    statistic.increment_exception_count()
            else:
                try:
                    test_map.execute_on_key(key, processor).result()
                    statistic.increment_execute_on_key_count()
                except Exception as e:
                    logging.info("Error in execute_on_key() : " + e.args[0])
                    statistic.increment_exception_count()

    thread_hang_statistics = {i: 0 for i in range(THREAD_COUNT_CONST)}

    def display_statistics(statistics):
        end_time = time.time() + hour_limit * 60 * 60
        while time.time() < end_time:
            time.sleep(STATS_DISPLAY_SECONDS_CONST)
            logging.info("-" * 40)

            total_operation_count = 0
            hanged_thread_ids = []
            for statistic in statistics:
                try:
                    current_statistic = statistic.get_and_reset()
                    total_operation_count += current_statistic.get_total_operation_count()
                    if current_statistic.get_total_operation_count() == 0:
                        hanged_thread_ids.append(current_statistic.id)
                        logging.info("Thread No Operation Error: " + str(current_statistic))
                except Exception as e:
                    logging.info(e.args[0])

            if len(hanged_thread_ids) == 0:
                logging.info("All threads worked without hanging")
            else:
                logging.info("{} threads hanged. IDs: {}".format(len(hanged_thread_ids), hanged_thread_ids))
                for thread_id in hanged_thread_ids:
                    thread_hang_statistics[thread_id] += 1

            logging.info("-" * 40)
            logging.info("Operations Per Second: " + str(total_operation_count / STATS_DISPLAY_SECONDS_CONST) + "\n")

    statistics = []
    threads = []

    for i in range(THREAD_COUNT_CONST):
        statistic = Statistic(i)
        statistics.append(statistic)
        thread = threading.Thread(target=soak_test_function, args=(statistic,))
        threads.append(thread)
        thread.start()

    display_thread = threading.Thread(target=display_statistics, args=(statistics,))
    display_thread.start()
    display_thread.join()

    for thread in threads:
        thread.join()

    client.shutdown()

    logging.info("* " * 20)
    logging.info("Soak test has finished!")
    logging.info("-" * 40)
    
    if sum(thread_hang_statistics.values()) == 0:
        logging.info("All threads worked without hanging")
    else:
        for key in thread_hang_statistics:
            if thread_hang_statistics[key] != 0:
                logging.info("Thread {} hanged {} times.".format(key, thread_hang_statistics[key]))

    time.sleep(10)
    thread_count_after = threading.active_count()
    logging.info("Thread count before: {}, after: {}".format(thread_count_before, thread_count_after))


if __name__ == "__main__":
    start_map_soak_test()
