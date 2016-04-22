import logging
import random
import sys
import threading
import time
from os.path import dirname

from hazelcast.config import SerializationConfig
from hazelcast.serialization import SerializationServiceV1

sys.path.append(dirname(dirname(dirname(__file__))))


def do_benchmark():
    MAP_NAME = "default"
    THREAD_COUNT = 1
    ENTRY_COUNT = 10 * 1000
    VALUE_SIZE = 100
    GET_PERCENTAGE = 40
    PUT_PERCENTAGE = 40

    VALUE = "x" * VALUE_SIZE

    logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
    logging.getLogger().setLevel(logging.INFO)
    logger = logging.getLogger("main")

    class ClientThread(threading.Thread):
        def __init__(self, name):
            threading.Thread.__init__(self, name=name)
            self.gets = 0
            self.puts = 0
            self.removes = 0
            self.setDaemon(True)
            self.my_map = dict()
            self.ss = SerializationServiceV1(serialization_config=SerializationConfig())

        def run(self):
            while True:
                key = int(random.random() * ENTRY_COUNT)
                operation = int(random.random() * 100)
                if operation < GET_PERCENTAGE:
                    key_data = self.ss.to_data(key)
                    val_dat = self.my_map.get(key_data, None)
                    self.ss.to_object(val_dat)
                    self.gets += 1
                elif operation < GET_PERCENTAGE + PUT_PERCENTAGE:
                    key_data = self.ss.to_data(key)
                    self.my_map[key_data] = self.ss.to_data(VALUE)
                    self.puts += 1
                else:
                    try:
                        key_data = self.ss.to_data(key)
                        del self.my_map[key_data]
                    except KeyError:
                        pass
                    self.removes += 1

    threads = [ClientThread("client-thread-%d" % i) for i in range(0, THREAD_COUNT)]
    for t in threads:
        t.start()

    start = time.time()
    counter = 1
    while counter < 3:
        time.sleep(5)
        print("ops per second : " + \
              str(sum([t.gets + t.puts + t.removes for t in threads]) / (time.time() - start)))
        for t in threads:
            print ("%s: put: %d get: %d: remove: %d" % (t.name, t.puts, t.gets, t.removes))
        counter += 1


if __name__ == '__main__':
    do_benchmark()
