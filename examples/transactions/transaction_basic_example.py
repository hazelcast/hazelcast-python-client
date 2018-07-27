import hazelcast
import logging
import time

if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    client = hazelcast.HazelcastClient()
    transaction = client.new_transaction(timeout=10)
    try:
        transaction.begin()
        transactional_map = transaction.get_map("transaction-map")
        print("Map: {}".format(transactional_map))

        transactional_map.put("1", "1")
        time.sleep(0.1)
        transactional_map.put("2", "2")

        transaction.commit()
    except Exception as ex:
        transaction.rollback()
        print("Transaction failed! {}".format(ex.args))
