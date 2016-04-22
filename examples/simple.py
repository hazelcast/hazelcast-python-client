import hazelcast
import logging


class A(object):

    def fnc(self):
        self._base()

    def _base(self):
        print "base"


class B(A):
    def _base(self):
        print"override"

if __name__ == '__main__':

    a = A()
    b = B()

    a.fnc()
    b.fnc()

    # logging.basicConfig()
    # logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
    # logging.getLogger().setLevel(logging.INFO)
    # config = hazelcast.ClientConfig()
    # Hazelcast.Address is the hostname or IP address, e.g. 'localhost:5701'
    # config.network_config.addresses.append('127.0.0.1:5701')
    # client = hazelcast.HazelcastClient(config)
    #
    # client.get_map("name")
