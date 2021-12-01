import binascii
import random
import threading
import time
import uuid

from collections.abc import Sequence, Iterable

from hazelcast.serialization import UUID_MSB_SHIFT, UUID_LSB_MASK, UUID_MSB_MASK


DEFAULT_ADDRESS = "127.0.0.1"
DEFAULT_PORT = 5701

MILLISECONDS_IN_SECONDS = 1000
NANOSECONDS_IN_SECONDS = 1e9


def check_not_none(val, message):
    if val is None:
        raise AssertionError(message)


def check_true(val, message):
    if not val:
        raise AssertionError(message)


def check_not_negative(val, message):
    if val < 0:
        raise AssertionError(message)


def check_not_empty(collection, message):
    if not collection:
        raise AssertionError(message)


def check_is_number(val, message="Number value expected"):
    if not isinstance(val, number_types):
        raise AssertionError(message)


def check_is_int(val, message="Int value expected"):
    if not isinstance(val, int):
        raise AssertionError(message)


def current_time():
    return time.time()


def current_time_in_millis():
    return to_millis(current_time())


def thread_id():
    return threading.currentThread().ident


def to_millis(seconds):
    if seconds is None:
        return -1
    return int(seconds * MILLISECONDS_IN_SECONDS)


def to_nanos(seconds):
    return int(seconds * NANOSECONDS_IN_SECONDS)


def validate_type(_type):
    if not isinstance(_type, type):
        raise ValueError("Serializer should be an instance of %s" % _type.__name__)


def validate_serializer(serializer, _type):
    if not issubclass(serializer, _type):
        raise ValueError("Serializer should be an instance of %s" % _type.__name__)


class AtomicInteger(object):
    """An Integer which can work atomically."""

    def __init__(self, initial=0):
        self._mux = threading.RLock()
        self._counter = initial

    def get_and_increment(self):
        """Returns the current value and increment it.

        Returns:
            int: Current value of AtomicInteger.
        """
        with self._mux:
            res = self._counter
            self._counter += 1
            return res

    def increment_and_get(self):
        """Increments the current value and returns it.

        Returns:
            int: Incremented value of AtomicInteger.
        """
        with self._mux:
            self._counter += 1
            return self._counter

    def get(self):
        """Returns the current value.

        Returns:
            int: The current value.
        """
        with self._mux:
            return self._counter

    def add(self, count):
        """Adds the given value to the current value.
        Args:
            count (int): The value to add.
        """
        with self._mux:
            self._counter += count


class ImmutableLazyDataList(Sequence):
    def __init__(self, list_data, to_object):
        super(ImmutableLazyDataList, self).__init__()
        self._list_data = list_data
        self._list_obj = [None] * len(self._list_data)
        self.to_object = to_object

    def __contains__(self, value):
        return super(ImmutableLazyDataList, self).__contains__(value)

    def __len__(self):
        return self._list_data.__len__()

    def __getitem__(self, index):
        val = self._list_obj[index]
        if not val:
            data = self._list_data[index]
            if isinstance(data, tuple):
                (key, value) = data
                self._list_obj[index] = (self.to_object(key), self.to_object(value))
            else:
                self._list_obj[index] = self.to_object(data)
        return self._list_obj[index]

    def __eq__(self, other):
        if not isinstance(other, Iterable):
            return False
        self._populate()
        return self._list_obj == other

    def _populate(self):
        for index, data in enumerate(self._list_data):
            if not self._list_obj[index]:
                self.__getitem__(index)

    def __repr__(self):
        self._populate()
        return str(self._list_obj)


# Serialization Utilities


def get_portable_version(portable, default_version):
    try:
        version = portable.get_class_version()
    except AttributeError:
        version = default_version
    return version


# Version utilities
UNKNOWN_VERSION = -1
MAJOR_VERSION_MULTIPLIER = 10000
MINOR_VERSION_MULTIPLIER = 100


def calculate_version(version_str):
    if not version_str:
        return UNKNOWN_VERSION

    main_parts = version_str.split("-")
    tokens = main_parts[0].split(".")

    if len(tokens) < 2:
        return UNKNOWN_VERSION

    try:
        major_coeff = int(tokens[0])
        minor_coeff = int(tokens[1])

        calculated_version = (
            major_coeff * MAJOR_VERSION_MULTIPLIER + minor_coeff * MINOR_VERSION_MULTIPLIER
        )

        if len(tokens) > 2:
            patch_coeff = int(tokens[2])
            calculated_version += patch_coeff

        return calculated_version
    except ValueError:
        return UNKNOWN_VERSION


def to_list(*args, **kwargs):
    return list(*args, **kwargs)


def to_signed(unsigned, bit_len):
    mask = (1 << bit_len) - 1
    if unsigned & (1 << (bit_len - 1)):
        return unsigned | ~mask
    return unsigned & mask


def get_attr_name(cls, value):
    for attr_name, attr_value in vars(cls).items():
        if attr_value == value:
            return attr_name
    return None


def _get_enum_value(cls, key):
    if isinstance(key, str):
        return getattr(cls, key, None)
    return None


def try_to_get_enum_value(value, enum_class):
    if get_attr_name(enum_class, value):
        # If the value given by the user corresponds
        # to value of the one of the enum members,
        # it is okay to set it directly
        return value
    else:
        # We couldn't find a enum member name for the
        # given value. Try to match the given config
        # option with enum member names and get value
        # associated with it.
        enum_value = _get_enum_value(enum_class, value)
        if enum_value is not None:
            return enum_value
        else:
            raise TypeError(
                "%s must be equal to one of the values or "
                "names of the members of the %s" % (value, enum_class.__name__)
            )


number_types = (int, float)
none_type = type(None)


class LoadBalancer(object):
    """Load balancer allows you to send operations to one of a number of endpoints (Members).
    It is up to the implementation to use different load balancing policies.

    If the client is configured with smart routing,
    only the operations that are not key based will be routed to the endpoint
    """

    def init(self, cluster_service):
        """Initializes the load balancer.

        Args:
            cluster_service (hazelcast.cluster.ClusterService): The cluster service to select members from
        """
        raise NotImplementedError("init")

    def next(self):
        """Returns the next member to route to.

        Returns:
            hazelcast.core.MemberInfo: the next member or ``None`` if no member is available.
        """
        raise NotImplementedError("next")


class _AbstractLoadBalancer(LoadBalancer):
    def __init__(self):
        self._cluster_service = None
        self._members = []

    def init(self, cluster_service):
        self._cluster_service = cluster_service
        cluster_service.add_listener(self._listener, self._listener, True)

    def _listener(self, _):
        self._members = self._cluster_service.get_members()


class RoundRobinLB(_AbstractLoadBalancer):
    """A load balancer implementation that relies on using round robin
    to a next member to send a request to.

    Round robin is done based on best effort basis, the order of members for concurrent calls to
    the next() is not guaranteed.
    """

    def __init__(self):
        super(RoundRobinLB, self).__init__()
        self._idx = 0

    def next(self):
        members = self._members
        if not members:
            return None

        n = len(members)
        idx = self._idx % n
        self._idx += 1
        return members[idx]


class RandomLB(_AbstractLoadBalancer):
    """A load balancer that selects a random member to route to."""

    def next(self):
        members = self._members
        if not members:
            return None
        idx = random.randrange(0, len(members))
        return members[idx]


class IterationType(object):
    """To differentiate users selection on result collection on map-wide
    operations like ``entry_set``, ``key_set``, ``values`` etc.
    """

    KEY = 0
    """Iterate over keys"""

    VALUE = 1
    """Iterate over values"""

    ENTRY = 2
    """Iterate over entries"""


class UUIDUtil(object):
    @staticmethod
    def to_bits(value):
        i = value.int
        most_significant_bits = to_signed(i >> UUID_MSB_SHIFT, 64)
        least_significant_bits = to_signed(i & UUID_LSB_MASK, 64)
        return most_significant_bits, least_significant_bits

    @staticmethod
    def from_bits(most_significant_bits, least_significant_bits):
        return uuid.UUID(
            int=(
                ((most_significant_bits << UUID_MSB_SHIFT) & UUID_MSB_MASK)
                | (least_significant_bits & UUID_LSB_MASK)
            )
        )


def int_from_bytes(buf):
    return int.from_bytes(buf, "big", signed=True)


def int_to_bytes(number):
    # Number of bytes to represent the number.
    # For numbers that don't have exactly 8n bit_length,
    # adding 8 and performing integer division with 8
    # let us get the correct length because
    # (8n + m + 8) // 8 = n + 0 + 1 (assuming m < 8).
    # For negative numbers, we add 1 to get rid of the
    # effects of the leading 1 (the sign bit).
    width = (8 + (number + (number < 0)).bit_length()) // 8
    return number.to_bytes(length=width, byteorder="big", signed=True)


def try_to_get_error_message(error):
    # If the error has a message attribute,
    # return it. If not, almost all of the
    # built-in errors (and Hazelcast Errors)
    # set the exception message as the first
    # parameter of args. If it is not there,
    # then return None.
    if hasattr(error, "message"):
        return error.message
    elif len(error.args) > 0:
        return error.args[0]
    return None


def _is_same_version(v1, v2):
    # Ignores the patch version
    return v1.major == v2.major and v1.minor == v2.minor


def _is_newer_version(v1, v2):
    # Ignores the patch version
    return v1.major > v2.major or (v1.major == v2.major and v1.minor > v2.minor)


def member_of_larger_same_version_group(members):
    """Finds a larger same-version group of data members from a collection of
    members and return a random member from the group.

    If the same-version groups have the same size, return a member from the
    newer group.

    Args:
        members (list[hazelcast.core.MemberInfo]): List of all members.

    Returns:
        hazelcast.core.MemberInfo: The chosen member or ``None``, if no data
        member is found.
    """
    # The members should have at most 2 different version (ignoring the patch
    # version).

    version0 = None
    version1 = None
    count0 = 0
    count1 = 0

    for member in members:
        if member.lite_member:
            continue

        v = member.version
        if not version0 or _is_same_version(version0, v):
            version0 = v
            count0 += 1
        elif not version1 or _is_same_version(version1, v):
            version1 = v
            count1 += 1
        else:
            raise ValueError(
                "More than 2 distinct member versions found: %s, %s, %s" % (version0, version1, v)
            )

    # no data members
    if count0 == 0:
        return None

    if count0 > count1 or count0 == count1 and _is_newer_version(version0, version1):
        count = count0
        version = version0
    else:
        count = count0
        version = version1

    # return a random member from the larger group
    random_member_idx = random.randrange(0, count)
    for member in members:
        if not member.lite_member and _is_same_version(version, member.version):
            random_member_idx -= 1
            if random_member_idx < 0:
                return member


def re_raise(exception, traceback):
    if exception.__traceback__ is not traceback:
        raise exception.with_traceback(traceback)

    raise exception
