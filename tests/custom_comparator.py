from hazelcast.core import Comparator
from hazelcast.serialization.api import IdentifiedDataSerializable
from hazelcast.util import ITERATION_TYPE


class CustomComparator(Comparator, IdentifiedDataSerializable):
    """
    Custom serializable Comparator to test paging predicate in non java clients.
    """
    FACTORY_ID = 66  # IdentifiedFactory
    CLASS_ID = 2

    def __init__(self, type=0, iteration_type=ITERATION_TYPE.KEY):
        self._type = type
        self._iteration_type = iteration_type

    def compare(self, entry1, entry2):
        """
        This method is used to determine order of entries when sorting.
        - If return value is a negative value, [entry1] comes after [entry2],
        - If return value is a positive value, [entry1] comes before [entry2],
        - If return value is 0, [entry1] and [entry2] are indistinguishable in this sorting mechanism.
            Their order with respect to each other is undefined.
        This method must always return the same result given the same pair of entries.
        :param entry1: (K,V pair), first entry
        :param entry2: (K,V pair), second entry
        :return: (int), order index
        """
        if self._iteration_type == ITERATION_TYPE.ENTRY:
            str1 = str(entry1[0]) + str(entry1[1])
            str2 = str(entry2[0]) + str(entry2[1])
        elif self._iteration_type == ITERATION_TYPE.VALUE:
            str1 = str(entry1[1])
            str2 = str(entry2[1])
        else:
            # iteration_type is ITERATION_TYPE.KEY or default
            str1 = str(entry1[0])
            str2 = str(entry2[0])

        if self._type == 0:
            if str1 > str2:
                return 1
            else:
                return -1 if str1 < str2 else 0
        elif self._type == 1:
            if str2 > str1:
                return 1
            else:
                return -1 if str2 < str1 else 0
        elif self._type == 2:
            if len(str1) > len(str2):
                return 1
            else:
                return -1 if len(str1) < len(str2) else 0
        else:
            return 0

    def get_factory_id(self):
        return self.FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID

    def read_data(self, object_data_input):
        self._type = object_data_input.read_int()
        self._iteration_type = ITERATION_TYPE(object_data_input.read_int())

    def write_data(self, object_data_output):
        object_data_output.write_int(self._type)
        object_data_output.write_int(self._iteration_type)