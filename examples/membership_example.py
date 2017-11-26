import logging
from time import sleep

import hazelcast


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
    logging.getLogger().setLevel(logging.INFO)
    logger = logging.getLogger("main")

    config = hazelcast.ClientConfig()
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

    config.group_config.name = "dev"
    config.group_config.password = "dev-pass"

    def member_added(e):
        print('member added event :{}'.format(e))

    def member_removed(e):
        print('member removed event :{}'.format(e))

    # member_added and member_removed functions will be called when cluster state changed
    config.add_membership_listener(member_added, member_removed, True)

    client = hazelcast.HazelcastClient(config)

    # This will start a new cluster member using remote-controller
    rc_member2 = rc.startMember(rc_cluster.id)

    # this will shutdown member-2 using remote-controller
    rc.shutdownMember(rc_cluster.id, rc_member2.uuid)

    # this will shutdown member-1 using remote-controller
    rc.shutdownMember(rc_cluster.id, rc_member.uuid)

    sleep(10)
    client.shutdown()
