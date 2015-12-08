import threading
import random
import time
import logging
import sys

from os.path import dirname
sys.path.append(dirname(dirname(__file__)))

import hazelcast


THREAD_COUNT = 1
ENTRY_COUNT = 10 * 1000
VALUE_SIZE = 100
GET_PERCENTAGE = 40
PUT_PERCENTAGE = 40

logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger("main")

config = hazelcast.Config()
config.username = "dev"
config.password = "dev-pass"
config.addresses.append("127.0.0.1:5701")
client = hazelcast.HazelcastClient(config)

lock = threading.Lock()
put_operation_count = 0
get_operation_count = 0
remove_operation_count = 0
counter = 1

def run():
    my_map = client.get_map("default")
    global get_operation_count
    global put_operation_count
    global remove_operation_count

    while True:
        key = int(random.random() * ENTRY_COUNT)
        operation = int(random.random() * 100)
        if operation < GET_PERCENTAGE:
            my_map.get(key)

            get_operation_count += 1
        elif operation < GET_PERCENTAGE + PUT_PERCENTAGE:
            my_map.put(key, "x" * VALUE_SIZE)

            put_operation_count += 1
        else:
            my_map.remove(key)
            remove_operation_count += 1

for i in range(0, THREAD_COUNT):
    t = threading.Thread(target=run)
    t.setDaemon(True)
    t.start()

while counter < 10:
    time.sleep(5)
    print "ops per second : " +\
          str((put_operation_count + get_operation_count + remove_operation_count) / 5)
    put_operation_count = 0
    get_operation_count = 0
    remove_operation_count = 0
    counter += 1
