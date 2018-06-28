import logging
import random
from time import sleep

import hazelcast
from hazelcast import six
from hazelcast.six.moves import range


def fill_map(hz_map, count=10):
    _map = {"key-%d" % x: "value-%d" % x for x in range(0, count)}
    for k, v in six.iteritems(_map):
        hz_map.put(k, v).result()
    return _map


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
    logging.getLogger().setLevel(logging.INFO)
    logger = logging.getLogger("main")

    config = hazelcast.ClientConfig()
    config.group_config.name = "dev"
    config.group_config.password = "dev-pass"
    try:
        from tests.hzrc.client import HzRemoteController
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

    my_map = client.get_map("map")
    six.print_(my_map)

    fill_map(my_map, 1000)

    six.print_("map.size", my_map.size().result())

    key = random.random()

    def put_callback(f):
        six.print_("map.put", f.result())
    my_map.put(key, "async_val").add_done_callback(put_callback)

    def contains_key_callback(f):
        six.print_("map.contains_key", f.result())
    my_map.contains_key(key).add_done_callback(contains_key_callback)

    sleep(10)
    client.shutdown()
    #
