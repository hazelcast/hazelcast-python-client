import logging
import os

from uuid import uuid4
from hazelcast.config import ClientConfig, PROTOCOL


def random_string():
    return str(uuid4())


def configure_logging(log_level=logging.INFO):
    logging.basicConfig(format='%(asctime)s%(msecs)03d [%(threadName)s][%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M:%S,")
    logging.getLogger().setLevel(log_level)


def event_collector():
    events = []

    def collector(e):
        events.append(e)

    collector.events = events
    return collector


def fill_map(map, size=10, key_prefix="key", value_prefix="val"):
    entries = dict()
    for i in range(size):
        entries[key_prefix + str(i)] = value_prefix + str(i)
    map.put_all(entries)
    return entries


def get_ssl_config(enable_ssl=False,
                   cafile=None,
                   certfile=None,
                   keyfile=None,
                   password=None,
                   protocol=PROTOCOL.TLS,
                   ciphers=None,
                   attempt_limit=1):
    config = ClientConfig()

    config.network_config.ssl_config.enabled = enable_ssl
    config.network_config.ssl_config.cafile = cafile
    config.network_config.ssl_config.certfile = certfile
    config.network_config.ssl_config.keyfile = keyfile
    config.network_config.ssl_config.password = password
    config.network_config.ssl_config.protocol = protocol
    config.network_config.ssl_config.ciphers = ciphers

    config.network_config.connection_attempt_limit = attempt_limit
    return config


def get_abs_path(cur_dir, file_name):
    return os.path.abspath(os.path.join(cur_dir, file_name))


def generate_key_owned_by_instance(client, instance):
    while True:
        key = random_string()
        partition_id = client.partition_service.get_partition_id(key)
        if client.partition_service.get_partition_owner(partition_id) == instance:
            return key


def set_attr(*args, **kwargs):
    def wrap_ob(ob):
        for name in args:
            setattr(ob, name, True)
        for name, value in kwargs.items():
            setattr(ob, name, value)
        return ob

    return wrap_ob


def open_connection_to_address(client, address):
    key = generate_key_owned_by_instance(client, address)
    m = client.get_map(random_string()).blocking()
    m.put(key, 0)
    m.destroy()

