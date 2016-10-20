import random
import time
import logging
import sys
import multiprocessing
from os.path import dirname

sys.path.append(dirname(dirname(dirname(__file__))))

import hazelcast


def do_benchmark():
    PROCESS_COUNT = 10
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

    class ClientProcess(multiprocessing.Process):
        def __init__(self, name, config, counts):
            multiprocessing.Process.__init__(self, name=name)
            self.counts = counts
            self.config = config
            self.daemon = True

        def run(self):
            client = hazelcast.HazelcastClient(config)
            my_map = client.get_map("default")
            while True:
                key = int(random.random() * ENTRY_COUNT)
                operation = int(random.random() * 100)
                if operation < GET_PERCENTAGE:
                    my_map.get(key)
                    self.counts[0] += 1
                elif operation < GET_PERCENTAGE + PUT_PERCENTAGE:
                    my_map.put(key, "x" * VALUE_SIZE)
                    self.counts[1] += 1
                else:
                    my_map.remove(key)
                    self.counts[2] += 1

    processes = [ClientProcess("client-process-%d" % i, config, multiprocessing.Array('i', 3)) for i in
                 xrange(0, PROCESS_COUNT)]
    for p in processes:
        p.start()

    start = time.time()
    counter = 1
    while counter < 1000:
        time.sleep(5)
        print "ops per second : " + \
              str(sum([sum(p.counts) for p in processes]) / (time.time() - start))
        # for p in processes:
        #     print ("%s: put: %d get: %d: remove: %d" % (p.name, p.counts[0], p.counts[1], p.counts[2]))
        counter += 1


if __name__ == '__main__':
    do_benchmark()
