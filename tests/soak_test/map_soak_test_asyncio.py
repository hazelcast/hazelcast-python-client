import argparse
import asyncio
import logging
import os
import random
import sys
import time
import typing
from collections import defaultdict

sys.path.append(os.path.join(os.path.dirname(__file__), "../../"))
# Making sure that hazelcast directory is in the sys.path so we can import modules from there in the command line.

from hazelcast.asyncio import HazelcastClient, Map
from hazelcast.predicate import between
from hazelcast.serialization.api import IdentifiedDataSerializable

if hasattr(time, "monotonic"):
    get_current_timestamp = time.monotonic
else:
    get_current_timestamp = time.time


TASK_COUNT = 256
ENTRY_COUNT = 10000
OBSERVATION_INTERVAL = 10.0


class SoakTestCoordinator:
    def __init__(self, test_duration, address):
        self.address = address
        self._task_count_before = len(asyncio.all_tasks())
        self._deadline = get_current_timestamp() + test_duration * 60 * 60
        self._lock = asyncio.Lock()
        # the following are protected by the lock above
        self._reached_deadline = False
        self._tests_failed = False

    async def start_tests(self):
        client, test_map = await self._init_client_map()
        logging.info("Soak test operations are starting!")
        logging.info("* " * 20 + "\n")
        test_runners = [TestRunner(i, self) for i in range(TASK_COUNT)]
        observer = OperationCountObserver(self, test_runners)
        async with asyncio.TaskGroup() as tg:
            observer_task = tg.create_task(observer.run())
            for runner in test_runners:
                tg.create_task(runner.run(test_map))

            await observer_task

        logging.info("* " * 20)
        logging.info("Soak test has finished!")
        logging.info("-" * 40)

        if await self.tests_failed():
            logging.info("Soak test failed!")
            return

        hang_counts = await observer.hang_counts()
        if not hang_counts:
            logging.info("All threads worked without hanging")
        else:
            for runner, hang_count in hang_counts:
                logging.info("Thread %s hanged %s times.", runner.id, hang_count)

        await client.shutdown()
        # wait for canceled tasks to expire
        await asyncio.sleep(1)
        task_count_after = len(asyncio.all_tasks())
        logging.info("Task count before: %s, after: %s", self._task_count_before, task_count_after)

    async def notify_error(self):
        async with self._lock:
            self._tests_failed = True

    async def should_continue_tests(self):
        async with self._lock:
            return not (self._reached_deadline or self._tests_failed)

    async def check_deadline(self):
        async with self._lock:
            now = get_current_timestamp()
            self._reached_deadline = now >= self._deadline

    async def tests_failed(self) -> bool:
        async with self._lock:
            return self._tests_failed

    async def _init_client_map(self) -> typing.Tuple[HazelcastClient, Map]:
        def no_op_listener(_):
            pass

        try:
            client = await HazelcastClient.create_and_start(
                cluster_members=[self.address],
                data_serializable_factories={
                    SimpleEntryProcessor.FACTORY_ID: {
                        SimpleEntryProcessor.CLASS_ID: SimpleEntryProcessor
                    }
                },
            )
            map = await client.get_map("test-map")
            await map.add_entry_listener(
                include_value=False,
                added_func=no_op_listener,
                removed_func=no_op_listener,
                updated_func=no_op_listener,
            )
            return client, map

        except Exception as e:
            logging.exception("Client failed to start")
            raise e


class TestRunner:
    def __init__(self, id, coordinator: SoakTestCoordinator):
        self.id = id
        self.coordinator = coordinator
        self.counter = OperationCounter()

    async def run(self, test_map):
        coordinator = self.coordinator
        processor = SimpleEntryProcessor("test")
        while await coordinator.should_continue_tests():
            key = str(random.randint(0, ENTRY_COUNT))
            value = str(random.randint(0, ENTRY_COUNT))
            operation = random.randint(0, 100)
            try:
                if operation < 30:
                    await test_map.get(key)
                elif operation < 60:
                    await test_map.put(key, value)
                elif operation < 80:
                    await test_map.values(between("this", 0, 10))
                else:
                    await test_map.execute_on_key(key, processor)

                await self.counter.increment()
            except Exception:
                await coordinator.notify_error()
                logging.exception("Unexpected error occurred in thread %s", self)
                return


class OperationCountObserver:
    def __init__(self, coordinator, test_runners):
        self.coordinator = coordinator
        self.test_runners = test_runners
        self._lock = asyncio.Lock()
        # the lock above protects the fields below
        self._hang_counts = defaultdict(int)

    async def hang_counts(self):
        async with self._lock:
            return self._hang_counts

    async def _increment_hang_count(self, runner):
        async with self._lock:
            self._hang_counts[runner] += 1

    async def run(self):
        while True:
            await asyncio.sleep(OBSERVATION_INTERVAL)
            await self.coordinator.check_deadline()
            if not await self.coordinator.should_continue_tests():
                break

            logging.info("-" * 40)
            op_count = 0
            hanged_runners = []

            for test_runner in self.test_runners:
                op_count_per_runner = await test_runner.counter.get_and_reset()
                op_count += op_count_per_runner
                if op_count == 0:
                    hanged_runners.append(test_runner)

            if not hanged_runners:
                logging.info("All threads worked without hanging")
            else:
                logging.info("%s threads hanged: %s", len(hanged_runners), hanged_runners)
                for hanged_runner in hanged_runners:
                    await self._increment_hang_count(hanged_runner)

            logging.info("-" * 40)
            logging.info("Operations Per Second: %s\n", op_count / OBSERVATION_INTERVAL)


class OperationCounter:
    def __init__(self):
        self._count = 0
        self._lock = asyncio.Lock()

    async def get_and_reset(self):
        async with self._lock:
            total = self._count
            self._count = 0
            return total

    async def increment(self):
        async with self._lock:
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


async def amain():
    arguments = parse_arguments()
    setup_logging(arguments.log_file)
    coordinator = SoakTestCoordinator(arguments.duration, arguments.address)
    await coordinator.start_tests()


if __name__ == "__main__":
    asyncio.run(amain())
