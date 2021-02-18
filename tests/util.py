import os
import time

from uuid import uuid4
from hazelcast.config import SSLProtocol
from hazelcast.six import PY3


def get_current_timestamp():
    """
    Get current timestamp.
    time.monotonic() is more resilient since it uses cpu clock rather than system clock changes
    python 3.3 and up supports time.monotonic(). Before python 3.5 and on gnu/hurd monotonic() is
    not available. Since we support support python3.4 and up regarding python3, there is a check
    for the monotonic() availability.

    Returns:
        If time.time() used, this function returns is number of seconds since 1970,
        if time.monotonic() is used, it still returns fractional seconds but its value alone
        is not meaningful, but it can still can be used take time difference for profiling.
    """
    if PY3 and hasattr(time, "monotonic"):
        return time.monotonic()
    else:
        return time.time()


def random_string():
    return str(uuid4())


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


def get_ssl_config(
        cluster_name,
        enable_ssl=False,
        cafile=None,
        certfile=None,
        keyfile=None,
        password=None,
        protocol=SSLProtocol.TLSv1_2,
        ciphers=None,
):
    config = {
        "cluster_name": cluster_name,
        "ssl_enabled": enable_ssl,
        "ssl_cafile": cafile,
        "ssl_certfile": certfile,
        "ssl_keyfile": keyfile,
        "ssl_password": password,
        "ssl_protocol": protocol,
        "ssl_ciphers": ciphers,
        "cluster_connect_timeout": 2,
    }
    return config


def get_abs_path(cur_dir, file_name):
    return os.path.abspath(os.path.join(cur_dir, file_name))


def wait_for_partition_table(client):
    m = client.get_map(random_string()).blocking()
    while not client._internal_partition_service._partition_table.partitions:
        m.put(random_string(), 0)
        time.sleep(0.1)


def generate_key_owned_by_instance(client, uuid):
    while True:
        key = random_string()
        data = client._serialization_service.to_data(key)
        partition_id = client.partition_service.get_partition_id(data)
        owner = str(client.partition_service.get_partition_owner(partition_id))
        if owner == uuid:
            return key


def set_attr(*args, **kwargs):
    def wrap_ob(ob):
        for name in args:
            setattr(ob, name, True)
        for name, value in kwargs.items():
            setattr(ob, name, value)
        return ob

    return wrap_ob


def open_connection_to_address(client, uuid):
    key = generate_key_owned_by_instance(client, uuid)
    m = client.get_map(random_string()).blocking()
    m.put(key, 0)
    m.destroy()


class LoggingContext(object):
    def __init__(self, logger, level):
        self.logger = logger
        self.level = level
        self.old_level = logger.level

    def __enter__(self):
        self.logger.setLevel(self.level)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.logger.setLevel(self.old_level)
