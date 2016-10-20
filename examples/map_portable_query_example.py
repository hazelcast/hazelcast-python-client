import logging
import random
from time import sleep

import hazelcast
from hazelcast.serialization.api import Portable
from hazelcast.serialization.predicate import sql

FACTORY_ID = 2


class Customer(Portable):
    CLASS_ID = 9

    def __init__(self, id=None, name=None, surname=None, mobile=None):
        self.id = id
        self.name = name
        self.surname = surname
        self.mobile = mobile

    def write_portable(self, writer):
        writer.write_int("id", self.id)
        writer.write_utf("name", self.name)
        writer.write_utf("surname", self.surname)
        writer.write_utf("mobile", self.mobile)

    def read_portable(self, reader):
        self.id = reader.read_int("id")
        self.name = reader.read_utf("name")
        self.surname = reader.read_utf("surname")
        self.mobile = reader.read_utf("mobile")

    def get_factory_id(self):
        return FACTORY_ID

    def get_class_id(self):
        return CLASS_ID


class SamplePortable(Portable):
    CLASS_ID = 10

    def __init__(self, param_str=None, param_int=None):
        self.param_str = param_str
        self.param_int = param_int

    def write_portable(self, writer):
        writer.write_utf("param-str", self.param_str)
        writer.write_int("param-int", self.param_int)

    def read_portable(self, reader):
        self.param_str = reader.read_utf("param-str")
        self.param_int = reader.read_int("param-int")

    def get_factory_id(self):
        return FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID

    def __str__(self):
        return "SamplePortable[ param-str:{} param-int:{} ]".format(self.param_str, self.param_int)

    def __eq__(self, other):
        return self.param_str == other.param_str and self.param_int == other.param_int


def fill_map(hz_map, count=10):
    s_map = {SamplePortable("key-%d" % x, x): SamplePortable("value-%d" % x, x) for x in xrange(0, count)}
    for k, v in s_map.iteritems():
        hz_map.put(k, v)
    return s_map


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
    logging.getLogger().setLevel(logging.INFO)
    logger = logging.getLogger("main")

    config = hazelcast.ClientConfig()
    config.group_config.name = "dev"
    config.group_config.password = "dev-pass"

    config.serialization_config.portable_factories[FACTORY_ID] = \
        {SamplePortable.CLASS_ID: SamplePortable}

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

    my_map = client.get_map("map").blocking()  # returns sync map, all map functions are blocking
    print(my_map)
    #
    _map = fill_map(my_map, 1000)
    # map_keys = _map.keys()
    #
    # predicate = sql("param-int >= 990")
    # key_set = my_map.key_set(predicate)
    #
    # print("map.size=", my_map.size())
    # print("result size=", len(key_set))
    # print('Iterate over all map:')
    #
    # for key in key_set:
    #     print "key:", key

    my_map_async = client.get_map("map")
    print("Map Size:", my_map_async.size().result())

    predicate2 = sql("param-str ILIKE 'value-2%'")

    def values_callback(f):
        result_set = f.result()
        print("Query Result Size:", len(result_set))
        for value in result_set:
            print "value:", value
    my_map_async.values(predicate2).add_done_callback(values_callback)


    sleep(10)
    client.shutdown()
