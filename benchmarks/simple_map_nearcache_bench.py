import logging
import sys
from os.path import dirname

import time

import hazelcast
from hazelcast.config import NearCacheConfig, IN_MEMORY_FORMAT

sys.path.append(dirname(dirname(dirname(__file__))))

MAP_NAME = "default"
THREAD_COUNT = 1
ENTRY_COUNT = 10000
VALUE_SIZE = 100
GET_PERCENTAGE = 100
PUT_PERCENTAGE = 0

VALUE = "x" * VALUE_SIZE


def init():
    logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
    logging.getLogger().setLevel(logging.INFO)
    logger = logging.getLogger("main")

    config = hazelcast.ClientConfig()
    config.group_config.name = "dev"
    config.group_config.password = "dev-pass"
    config.network_config.addresses.append("127.0.0.1")

    near_cache_config = NearCacheConfig(MAP_NAME)
    near_cache_config.in_memory_format = IN_MEMORY_FORMAT.OBJECT
    config.add_near_cache_config(near_cache_config)

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

    my_map = client.get_map(MAP_NAME).blocking()

    print "START INIT"
    for key in xrange(0, ENTRY_COUNT):
        my_map.put(key, VALUE)
    for key in xrange(0, ENTRY_COUNT):
        my_map.get(key)
    print "INIT COMPLETE"
    return my_map


def bench(my_map):
    start = time.time()
    hit = my_map._near_cache._cache_hit
    for key in xrange(0, ENTRY_COUNT):
        my_map.get(key)
    print "op / sec :", ENTRY_COUNT / (time.time() - start), "hit:", my_map._near_cache._cache_hit-hit


if __name__ == '__main__':
    _map = init()
    bench(_map)
