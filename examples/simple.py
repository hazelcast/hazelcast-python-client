import hazelcast
import logging

if __name__ == '__main__':
    logging.basicConfig()
    # logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
    logging.getLogger().setLevel(logging.INFO)
    config = hazelcast.ClientConfig()
    # Hazelcast.Address is the hostname or IP address, e.g. 'localhost:5701'
    config.network_config.addresses.append('127.0.0.1:5701')
    client = hazelcast.HazelcastClient(config)

    # client.get_map("name")
