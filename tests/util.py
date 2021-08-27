import os
import time

from uuid import uuid4
from hazelcast import __version__
from hazelcast.config import SSLProtocol
from hazelcast.util import calculate_version

from tests.hzrc.ttypes import Lang

# time.monotonic() is more consistent since it uses cpu clock rather than system clock. Use it if available.
if hasattr(time, "monotonic"):
    get_current_timestamp = time.monotonic
else:
    get_current_timestamp = time.time


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
    while not client.partition_service.get_partition_owner(0):
        m.put(random_string(), 0)
        time.sleep(0.1)


def generate_key_owned_by_instance(client, uuid):
    while True:
        key = random_string()
        partition_id = client.partition_service.get_partition_id(key)
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


def mark_server_version_at_least(test, client, version):
    if compare_server_version(client, version) < 0:
        test.skipTest("Expected a newer server")


def mark_server_version_at_most(test, client, version):
    if compare_server_version(client, version) >= 0:
        test.skipTest("Expected an older server")


def compare_server_version(client, version):
    """Returns
    - 0 if they are equal
    - positive number if server version is newer than the version
    - negative number if server version is older than the version
    """
    connection = client._connection_manager.get_random_connection()
    server_version = connection.server_version
    version = calculate_version(version)
    return server_version - version


def compare_server_version_with_rc(rc, version):
    """Returns
    - 0 if they are equal
    - positive number if server version is newer than the version
    - negative number if server version is older than the version
    """
    script = """result=com.hazelcast.instance.GeneratedBuildProperties.VERSION;"""
    result = rc.executeOnController(None, script, Lang.JAVASCRIPT)
    server_version = calculate_version(result.result.decode())
    version = calculate_version(version)
    return server_version - version


def mark_client_version_at_least(test, version):
    if compare_client_version(version) < 0:
        test.skipTest("Expected a newer client")


def mark_client_version_at_most(test, version):
    if compare_client_version(version) >= 0:
        test.skipTest("Expected an older client")


def compare_client_version(version):
    """Returns
    - 0 if they are equal
    - positive number if client version is newer than the version
    - negative number if client version is older than the version
    """
    client_version = calculate_version(__version__)
    version = calculate_version(version)
    return client_version - version


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
