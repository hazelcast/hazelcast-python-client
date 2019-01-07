import hazelcast
import logging

if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    client = hazelcast.HazelcastClient()

    pn_counter = client.get_pn_counter("pn-counter").blocking()

    for i in range(10):
        print("Added {} to the counter. Current value is {}".format(i, pn_counter.add_and_get(i)))

    print("Incremented the counter after getting the current value. "
          "Old value is {}".format(pn_counter.get_and_increment()))

    print("Final value is {}".format(pn_counter.get()))
