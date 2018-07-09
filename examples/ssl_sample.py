import logging
import os
import hazelcast
from hazelcast import six

if __name__ == "__main__":
    logging.basicConfig(format="%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s", datefmt="%H:%M%:%S,")
    logging.getLogger().setLevel(logging.INFO)
    logger = logging.getLogger("main")

    config = hazelcast.ClientConfig()
    try:
        from tests.hzrc.client import HzRemoteController
        rc = HzRemoteController("127.0.0.1", "9701")

        if not rc.ping():
            logger.info("Remote controller server is not running. Exiting...")
            exit()
        logger.info("Remote controller server OK.")
        xml_path = os.path.abspath("../tests/ssl/hazelcast-ssl.xml")
        f = open(xml_path, "r")
        xml_config = f.read()
        f.close()
        rc_cluster = rc.createCluster(None, xml_config)
        rc_member = rc.startMember(rc_cluster.id)

        config.network_config.addresses.append('{}:{}'.format(rc_member.host, rc_member.port))
    except (ImportError, NameError):
        config.network_config.addresses.append('127.0.0.1')

    config.group_config.name = "dev"
    config.group_config.password = "dev-pass"

    # SSL Config
    ssl_config = hazelcast.SSLConfig()
    ssl_config.enabled = True
    ssl_config.cafile = os.path.abspath("../tests/ssl/server1-cert.pem")
    ssl_config.certfile = os.path.abspath("../tests/ssl/client1-cert.pem")
    ssl_config.keyfile = os.path.abspath("../tests/ssl/client1-key.pem")
    ssl_config.hostname = "foo.bar.com"

    config.network_config.ssl_config = ssl_config

    # Start a new Hazelcast client with SSL configuration.
    client = hazelcast.HazelcastClient(config)

    hz_map = client.get_map("hz-map")
    hz_map.put("key", "value")

    six.print_(hz_map.get("key").result())

    rc.shutdownMember(rc_cluster.id, rc_member.uuid)

    client.shutdown()
