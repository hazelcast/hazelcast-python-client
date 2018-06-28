import logging
from time import sleep

import hazelcast
from hazelcast import six

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
    logging.getLogger().setLevel(logging.INFO)
    logger = logging.getLogger("main")

    config = hazelcast.ClientConfig()
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

    config.group_config.name = "dev"
    config.group_config.password = "dev-pass"

    def lifecycle_state_changed(lifecycle_state_str):
        #  One of these states will be received
        # from hazelcast.lifecycle import LIFECYCLE_STATE_SHUTDOWN, LIFECYCLE_STATE_SHUTTING_DOWN, LIFECYCLE_STATE_CONNECTED, \
        #     LIFECYCLE_STATE_STARTING, LIFECYCLE_STATE_DISCONNECTED
        six.print_(lifecycle_state_str)

    # lifecycle_state_changed function will be called with Lifecycle state as parameter when lifecycle state change
    config.add_lifecycle_listener(lifecycle_state_changed)

    client = hazelcast.HazelcastClient(config)

    sleep(10)
    client.shutdown()
