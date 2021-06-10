import hazelcast
import time

client = hazelcast.HazelcastClient()

with client.new_transaction(timeout=10) as transaction:
    transactional_map = transaction.get_map("transactional-map")
    print("Created map:", transactional_map)

    transactional_map.put("1", "1")
    time.sleep(0.1)
    transactional_map.put("2", "2")
