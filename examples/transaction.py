import logging

import sys
from os.path import dirname

sys.path.append(dirname(dirname(__file__)))
import hazelcast

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s%(msecs)03d [%(name)s] %(levelname)s: %(message)s', datefmt="%H:%M%:%S,")
    logging.getLogger().setLevel(logging.DEBUG)
    logger = logging.getLogger("main")

    client = hazelcast.HazelcastClient()

    t = client.new_transaction()
    t.begin()
    try:
        map = t.get_map("test")
        print(map)
        t.commit()
    except:
        logger.exception("Exception in transaction")
        t.rollback()
