import threading
import random
import time
import logging
import sys

from os.path import dirname


sys.path.append(dirname(dirname(dirname(__file__))))

import hazelcast

REQ_COUNT = 20000
ENTRY_COUNT = 10 * 1000
VALUE_SIZE = 10000
GET_PERCENTAGE = 40
PUT_PERCENTAGE = 40

logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
logging.getLogger().setLevel(logging.INFO)
logger = logging.getLogger("main")

config = hazelcast.ClientConfig()
config.group_config.name = "dev"
config.group_config.password = "dev-pass"
config.network_config.addresses.append("127.0.0.1:5701")
client = hazelcast.HazelcastClient(config)

class Test(object):
    ops = 0

    def get_cb(self, _):
        self.ops += 1

    def put_cb(self, _):
        self.ops += 1

    def remove_cb(self, _):
        self.ops += 1

    def run(self):
        my_map = client.get_map("default")
        for _ in xrange(0, REQ_COUNT):
            key = int(random.random() * ENTRY_COUNT)
            operation = int(random.random() * 100)
            if operation < GET_PERCENTAGE:
                my_map.get_async(key, self.get_cb)
            elif operation < GET_PERCENTAGE + PUT_PERCENTAGE:
                my_map.put_async(key, "x" * VALUE_SIZE, -1, self.put_cb)
            else:
                my_map.remove_async(key, self.remove_cb)
t = Test()
start = time.time()
t.run()
while t.ops != REQ_COUNT:
    time.sleep(0.01)
print("ops per second: %d" % (t.ops/(time.time()-start)))
