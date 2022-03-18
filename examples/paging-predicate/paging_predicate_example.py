import logging
import hazelcast
import random

from hazelcast import predicate
from hazelcast.core import HazelcastJsonValue
from hazelcast.serialization.api import IdentifiedDataSerializable

logging.basicConfig(level=logging.INFO)


class AgeComparator(IdentifiedDataSerializable):
    """
    This is just a marker class that identifies which comparator
    to use on the member.

    It also carries a boolean flag to determine the order of
    sorting done on the member side.

    It must implement the counterpart of the interface
    implemented by the Java code and have the same class and
    factory id.
    """

    def __init__(self, reverse=False):
        self._reverse = reverse

    def write_data(self, object_data_output):
        object_data_output.write_boolean(self._reverse)

    def read_data(self, object_data_input):
        self._reverse = object_data_input.read_boolean()

    def get_factory_id(self):
        return 1

    def get_class_id(self):
        return 1


client = hazelcast.HazelcastClient()

students = client.get_map("students").blocking()

# Populate the map with some random data.
for i in range(42):
    students.set(
        i,
        HazelcastJsonValue(
            {
                "student_id": i,
                "age": random.randrange(8, 24),
            }
        ),
    )

# Use the paging predicate with true predicate
# to get all students with the page size of 10.
# It also uses the custom comparator we have
# written and sorts the values in ascending
# order of age.
paging_predicate = predicate.paging(
    predicate=predicate.true(),
    page_size=10,
    comparator=AgeComparator(),
)
print(students.values(paging_predicate))

# Set up the next page and fetch it.
paging_predicate.next_page()
print(students.values(paging_predicate))

# This time, we will fetch students with the
# student_id between 10 and 40 with the page size
# of 5. We will also make use of the custom comparator
# and sort the results in descending order of age.
paging_predicate = predicate.paging(
    predicate=predicate.between("student_id", 10, 40),
    page_size=5,
    comparator=AgeComparator(reverse=True),
)
print(students.values(paging_predicate))

# Set up the next page and fetch it.
paging_predicate.next_page()
print(students.values(paging_predicate))

client.shutdown()
