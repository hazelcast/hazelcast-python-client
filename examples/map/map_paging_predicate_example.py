import hazelcast
from hazelcast.serialization.api import IdentifiedDataSerializable
from hazelcast.serialization.predicate import paging, true

client = hazelcast.HazelcastClient()

m1 = client.get_map("m1").blocking()

m1.put_all({
    "a": 1,
    "b": 2,
    "c": 3,
    "d": 4,
    "e": 5,
    "f": 6,
    "g": 7,
})

size = m1.size()
print("Added %s elements" % size)

# When using paging predicate without a comparator,
# server sorts the entries according to their default
# orderings. In this particular case, values will be
# sorted in ascending order.
predicate = paging(true(), 2)  # Get all values with page size of 2

# Prints pages of size 2 in the [1, 2], [3, 4], [5, 6], [7] order
for i in range(4):
    values = m1.values(predicate)
    print("Page %s:%s" % (i, values))
    predicate.next_page()  # Call next_page on predicate to get the next page on the next iteration


# If you want to sort results differently, you have to use
# a comparator. Comparator will be serialized and sent
# to server. Server will do the sorting accordingly. Hence,
# the implementation of the comparator must be defined on the
# server side and registered as a Portable or IdentifiedDataSerializable
# before the server starts.

class ReversedKeyComparator(IdentifiedDataSerializable):
    """
    This class is simply a marker implementation
    to tell server which comparator to use.
    Its implementation must be defined on the
    server side. A sample server side implementation
    is provided at the end of file.
    """
    def get_class_id(self):
        return 1

    def get_factory_id(self):
        return 1

    def write_data(self, object_data_output):
        pass

    def read_data(self, object_data_input):
        pass


predicate = paging(true(), 2, ReversedKeyComparator())

# Prints pages of size 2 in the [7, 6], [5, 4], [3, 2], [1] order
for i in range(4):
    values = m1.values(predicate)
    print("Page %s:%s" % (i, values))
    predicate.next_page()  # Call next_page on predicate to get the next page on the next iteration

client.shutdown()

# A sample implementation of the comparator that
# sorts the values according the reverse of
# the alphabetical order of keys.

# class ReversedKeyComparator implements Comparator<Map.Entry<String, Integer>>, IdentifiedDataSerializable {
#
#     @Override
#     public int compare(Map.Entry<String, Integer> entry1, Map.Entry<String, Integer> entry2) {
#         // Reverse the order. The main comparator logic
#         // goes to here.
#         return entry2.getValue().compareTo(entry1.getValue());
#     }
#
#     @Override
#     public int getFactoryId() {
#         return 1;
#     }
#
#     @Override
#     public int getClassId() {
#         return 1;
#     }
#
#     @Override
#     public void writeData(ObjectDataOutput out) {
#
#     }
#
#     @Override
#     public void readData(ObjectDataInput in) {
#
#     }
# }
