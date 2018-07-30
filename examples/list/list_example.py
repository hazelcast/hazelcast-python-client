import hazelcast
import logging

if __name__ == "__main__":
    logging.basicConfig()
    logging.getLogger().setLevel(logging.INFO)

    client = hazelcast.HazelcastClient()

    my_list = client.get_list("cities-list")

    my_list.add("Tokyo")
    my_list.add("Paris")
    my_list.add("London")
    my_list.add("New York")
    my_list.add("Istanbul")

    print("List size: {}".format(my_list.size().result()))
    print("First element: {}".format(my_list.get(0).result()))
    print("Contains Istanbul: {}".format(my_list.contains("Istanbul").result()))
    print("Sublist: {}".format(my_list.sub_list(3, 5).result()))

    my_list.remove("Tokyo")
    print("Final size: {}".format(my_list.size().result()))

    client.shutdown()
