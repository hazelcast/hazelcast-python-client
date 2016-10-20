import threading
import random
import time
import logging
import sys
from os.path import dirname

sys.path.append(dirname(dirname(dirname(__file__))))

import hazelcast


def do_benchmark():
    THREAD_COUNT = 1
    ENTRY_COUNT = 10 * 1000
    VALUE_SIZE = 100
    GET_PERCENTAGE = 40
    PUT_PERCENTAGE = 40

    VALUE = "x" * VALUE_SIZE

    logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
    logging.getLogger().setLevel(logging.INFO)
    logger = logging.getLogger("main")

    config = hazelcast.ClientConfig()
    config.group_config.name = "dev"
    config.group_config.password = "dev-pass"

    try:
        from hzrc.client import HzRemoteController

        rc = HzRemoteController('127.0.0.1', '9701')

        if not rc.ping():
            logger.info("Remote Controller Server not running... exiting.")
            exit()
        logger.info("Remote Controller Server OK...")
        rc_cluster = rc.createCluster(None, None)
        rc_member = rc.startMember(rc_cluster.id)
        config.network_config.addresses.append('{}:{}'.format(rc_member.host, rc_member.port))
    except (ImportError, NameError):
        config.network_config.addresses.append('127.0.0.1')

    client = hazelcast.HazelcastClient(config)

    class ClientThread(threading.Thread):
        def __init__(self, name):
            threading.Thread.__init__(self, name=name)
            self.gets = 0
            self.puts = 0
            self.removes = 0
            self.setDaemon(True)

        def run(self):
            my_map = client.get_map("default").blocking()
            while True:
                key = int(random.random() * ENTRY_COUNT)
                operation = int(random.random() * 100)
                if operation < GET_PERCENTAGE:
                    my_map.get(key)
                    self.gets += 1
                elif operation < GET_PERCENTAGE + PUT_PERCENTAGE:
                    my_map.put(key, VALUE)
                    self.puts += 1
                else:
                    my_map.remove(key)
                    self.removes += 1

    threads = [ClientThread("client-thread-%d" % i) for i in range(0, THREAD_COUNT)]
    for t in threads:
        t.start()

    start = time.time()
    counter = 1
    while counter < 1000:
        time.sleep(5)
        print("ops per second : " + \
              str(sum([t.gets + t.puts + t.removes for t in threads]) / (time.time() - start)))
        for t in threads:
            print ("%s: put: %d get: %d: remove: %d" % (t.name, t.puts, t.gets, t.removes))
        counter += 1


if __name__ == '__main__':
    do_benchmark()
