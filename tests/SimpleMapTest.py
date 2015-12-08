import unittest, threading, random, time
import hazelcast
import logging


class SimpleMapTest(unittest.TestCase):
    lock = threading.Lock()
    putOperationCount = 0
    getOperationCount = 0
    removeOperationCount = 0
    THREAD_COUNT = 1
    ENTRY_COUNT = 10 * 1000
    VALUE_SIZE = 100
    GET_PERCENTAGE = 20
    PUT_PERCENTAGE = 80
    counter = 1

    def test_01_performance(self):
        logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
        logging.getLogger().setLevel(logging.INFO)
        logger = logging.getLogger("main")

        config = hazelcast.Config()
        config.username = "dev"
        config.password = "dev-pass"
        config.addresses.append("127.0.0.1:5701")
        client = hazelcast.HazelcastClient(config)

        class ClientThread(threading.Thread):
            def run(self):
                mymap = client.get_map("default")
                while True:
                    key = int(random.random() * SimpleMapTest.ENTRY_COUNT)
                    operation = int(random.random() * 100)
                    if operation < SimpleMapTest.GET_PERCENTAGE:
                        mymap.get(key)
                        SimpleMapTest.getOperationCount += 1
                    elif operation < SimpleMapTest.GET_PERCENTAGE + SimpleMapTest.PUT_PERCENTAGE:
                        mymap.put(key, "x" * SimpleMapTest.VALUE_SIZE)
                        SimpleMapTest.putOperationCount += 1
                    else:
                        mymap.remove(key)
                        SimpleMapTest.removeOperationCount += 1

        for i in range(0, SimpleMapTest.THREAD_COUNT):
            ClientThread().start()
        while SimpleMapTest.counter < 10:
            time.sleep(5)
            print "ops per second : " + str((
                                                SimpleMapTest.putOperationCount + SimpleMapTest.getOperationCount + SimpleMapTest.removeOperationCount) / 5)
            SimpleMapTest.putOperationCount = 0
            SimpleMapTest.getOperationCount = 0
            SimpleMapTest.removeOperationCount = 0
            SimpleMapTest.counter += 1
