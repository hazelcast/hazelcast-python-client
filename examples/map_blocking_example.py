import logging
import random
from time import sleep

import hazelcast
from hazelcast import six
from hazelcast.six.moves import range


def fill_map(hz_map, count=10):
    _map = {"key-%d" % x: "value-%d" % x for x in range(0, count)}
    for k, v in six.iteritems(_map):
        hz_map.put(k, v)
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

    my_map = client.get_map("map").blocking()# returns sync map, all map functions are blocking
    six.print_(my_map)

    fill_map(my_map, 1000)

    six.print_("map.size", my_map.size())

    key = random.random()
    six.print_("map.put", my_map.put(key, "value"))
    six.print_("map.contains_key", my_map.contains_key(key))
    six.print_("map.get", my_map.get(key))
    six.print_("map.size", my_map.size())
    six.print_("map.remove", my_map.remove(key))
    six.print_("map.size", my_map.size())
    six.print_("map.contains_key", my_map.contains_key(key))

    six.print_('Iterate over all map:')

    for key, value in my_map.entry_set():
        six.print_("key:", key, "value:", value)

    sleep(10)
    client.shutdown()
    #
