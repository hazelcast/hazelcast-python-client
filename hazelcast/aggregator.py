import typing

from hazelcast.core import MapEntry
from hazelcast.serialization.api import IdentifiedDataSerializable
from hazelcast.types import AggregatorResultType, KeyType, ValueType

_AGGREGATORS_FACTORY_ID = -29


class Aggregator(typing.Generic[AggregatorResultType]):
    """Marker base class for all aggregators.

    Aggregators allow computing a value of some function (e.g sum or max) over
    the stored map entries. The computation is performed in a fully distributed
    manner, so no data other than the computed value is transferred to the
    client, making the computation fast.
    """

    pass


class _AbstractAggregator(Aggregator[AggregatorResultType], IdentifiedDataSerializable):
    def __init__(self, attribute_path=None):
        self._attribute_path = attribute_path

    def write_data(self, object_data_output):
        raise NotImplementedError("write_data")

    def read_data(self, object_data_input):
        pass

    def get_factory_id(self):
        return _AGGREGATORS_FACTORY_ID

    def get_class_id(self):
        raise NotImplementedError("get_class_id")


class _CountAggregator(_AbstractAggregator[int]):
    def write_data(self, object_data_output):
        object_data_output.write_string(self._attribute_path)
        object_data_output.write_long(0)

    def get_class_id(self):
        return 4


class _DistinctValuesAggregator(_AbstractAggregator[typing.Set[AggregatorResultType]]):
    def write_data(self, object_data_output):
        object_data_output.write_string(self._attribute_path)
        object_data_output.write_int(0)

    def get_class_id(self):
        return 5


class _DoubleAverageAggregator(_AbstractAggregator[float]):
    def write_data(self, object_data_output):
        object_data_output.write_string(self._attribute_path)
        object_data_output.write_double(0)
        object_data_output.write_long(0)

    def get_class_id(self):
        return 6


class _DoubleSumAggregator(_AbstractAggregator[float]):
    def write_data(self, object_data_output):
        object_data_output.write_string(self._attribute_path)
        object_data_output.write_double(0)

    def get_class_id(self):
        return 7


class _FixedPointSumAggregator(_AbstractAggregator[int]):
    def write_data(self, object_data_output):
        object_data_output.write_string(self._attribute_path)
        object_data_output.write_long(0)

    def get_class_id(self):
        return 8


class _FloatingPointSumAggregator(_AbstractAggregator[float]):
    def write_data(self, object_data_output):
        object_data_output.write_string(self._attribute_path)
        object_data_output.write_double(0)

    def get_class_id(self):
        return 9


class _IntegerAverageAggregator(_AbstractAggregator[int]):
    def write_data(self, object_data_output):
        object_data_output.write_string(self._attribute_path)
        object_data_output.write_long(0)
        object_data_output.write_long(0)

    def get_class_id(self):
        return 10


class _IntegerSumAggregator(_AbstractAggregator[int]):
    def write_data(self, object_data_output):
        object_data_output.write_string(self._attribute_path)
        object_data_output.write_long(0)

    def get_class_id(self):
        return 11


class _LongAverageAggregator(_AbstractAggregator[int]):
    def write_data(self, object_data_output):
        object_data_output.write_string(self._attribute_path)
        object_data_output.write_long(0)
        object_data_output.write_long(0)

    def get_class_id(self):
        return 12


class _LongSumAggregator(_AbstractAggregator[int]):
    def write_data(self, object_data_output):
        object_data_output.write_string(self._attribute_path)
        object_data_output.write_long(0)

    def get_class_id(self):
        return 13


class _MaxAggregator(_AbstractAggregator[AggregatorResultType]):
    def write_data(self, object_data_output):
        object_data_output.write_string(self._attribute_path)
        object_data_output.write_object(None)

    def get_class_id(self):
        return 14


class _MinAggregator(_AbstractAggregator[AggregatorResultType]):
    def write_data(self, object_data_output):
        object_data_output.write_string(self._attribute_path)
        object_data_output.write_object(None)

    def get_class_id(self):
        return 15


class _NumberAverageAggregator(_AbstractAggregator[float]):
    def write_data(self, object_data_output):
        object_data_output.write_string(self._attribute_path)
        object_data_output.write_double(0)
        object_data_output.write_long(0)

    def get_class_id(self):
        return 16


class _MaxByAggregator(_AbstractAggregator[MapEntry[KeyType, ValueType]]):
    def write_data(self, object_data_output):
        object_data_output.write_string(self._attribute_path)
        object_data_output.write_object(None)
        object_data_output.write_object(None)

    def get_class_id(self):
        return 17


class _MinByAggregator(_AbstractAggregator[MapEntry[KeyType, ValueType]]):
    def write_data(self, object_data_output):
        object_data_output.write_string(self._attribute_path)
        object_data_output.write_object(None)
        object_data_output.write_object(None)

    def get_class_id(self):
        return 18


def count(attribute_path: str = None) -> Aggregator[int]:
    """Creates an aggregator that counts the input values.

    Accepts ``None`` input values and ``None`` extracted values.

    Args:
        attribute_path: Extracts values from this path, if given.

    Returns:
        An aggregator that counts the input values.
    """
    return _CountAggregator(attribute_path)


def distinct(attribute_path: str = None) -> Aggregator[typing.Set[AggregatorResultType]]:
    """Creates an aggregator that calculates the distinct set of input values.

    Accepts ``None`` input values and ``None`` extracted values.

    Args:
        attribute_path: Extracts values from this path, if given.

    Returns:
        An aggregator that calculates the distinct set of input values.
    """
    return _DistinctValuesAggregator(attribute_path)


def double_avg(attribute_path: str = None) -> Aggregator[float]:
    """Creates an aggregator that calculates the average of the input values.

    Does NOT accept ``None`` input values or ``None`` extracted values.

    Since the server-side implementation is in Java, values stored in the
    Map must be of type ``double`` (primitive or boxed) in Java or of a type
    that can be converted to that. That means, one should be able to use this
    aggregator with ``float`` or ``int`` values sent from the Python client
    unless they are out of range for ``double`` type in Java.

    Args:
        attribute_path: Extracts values from this path, if given.

    Returns:
        An aggregator that calculates the average of the input values.
    """
    return _DoubleAverageAggregator(attribute_path)


def double_sum(attribute_path: str = None) -> Aggregator[float]:
    """Creates an aggregator that calculates the sum of the input values.

    Does NOT accept ``None`` input values or ``None`` extracted values.

    Since the server-side implementation is in Java, values stored in the
    Map must be of type ``double`` (primitive or boxed) in Java or of a type
    that can be converted to that. That means, one should be able to use this
    aggregator with ``float`` or ``int`` values sent from the Python client
    unless they are out of range for ``double`` type in Java.

    Args:
        attribute_path: Extracts values from this path, if given.

    Returns:
        An aggregator that calculates the sum of the input values.
    """
    return _DoubleSumAggregator(attribute_path)


def fixed_point_sum(attribute_path: str = None) -> Aggregator[int]:
    """Creates an aggregator that calculates the sum of the input values.

    Does NOT accept ``None`` input values or ``None`` extracted values.

    Accepts generic number input values. That means, one should be able to
    use this aggregator with ``float`` or ``int`` value sent from the Python
    client unless they are out of range for ``long`` type in Java.

    Args:
        attribute_path: Extracts values from this path, if given.

    Returns:
        An aggregator that calculates the sum of the input values.
    """
    return _FixedPointSumAggregator(attribute_path)


def floating_point_sum(attribute_path: str = None) -> Aggregator[float]:
    """Creates an aggregator that calculates the sum of the input values.

    Does NOT accept ``None`` input values or ``None`` extracted values.

    Accepts generic number input values. That means, one should be able to
    use this aggregator with ``float`` or ``int`` value sent from the Python
    client unless they are out of range for ``double`` type in Java.

    Args:
        attribute_path: Extracts values from this path, if given.

    Returns:
        An aggregator that calculates the sum of the input values.
    """
    return _FloatingPointSumAggregator(attribute_path)


def int_avg(attribute_path: str = None) -> Aggregator[int]:
    """Creates an aggregator that calculates the average of the input values.

    Does NOT accept ``None`` input values or ``None`` extracted values.

    Since the server-side implementation is in Java, values stored in the
    Map must be of type ``int`` (primitive or boxed) in Java or of a type
    that can be converted to that. That means, one should be able to use this
    aggregator with ``int`` values sent from the Python client unless they
    are out of range for ``int`` type in Java.

    Args:
        attribute_path: Extracts values from this path, if given.

    Returns:
        An aggregator that calculates the average of the input values.
    """
    return _IntegerAverageAggregator(attribute_path)


def int_sum(attribute_path: str = None) -> Aggregator[int]:
    """Creates an aggregator that calculates the sum of the input values.

    Does NOT accept ``None`` input values or ``None`` extracted values.

    Since the server-side implementation is in Java, values stored in the
    Map must be of type ``int`` (primitive or boxed) in Java or of a type
    that can be converted to that. That means, one should be able to use this
    aggregator with ``int`` values sent from the Python client unless they
    are out of range for ``int`` type in Java.

    Args:
        attribute_path: Extracts values from this path, if given.

    Returns:
        An aggregator that calculates the sum of the input values.
    """
    return _IntegerSumAggregator(attribute_path)


def long_avg(attribute_path: str = None) -> Aggregator[int]:
    """Creates an aggregator that calculates the average of the input values.

    Does NOT accept ``None`` input values or ``None`` extracted values.

    Since the server-side implementation is in Java, values stored in the
    Map must be of type ``long`` (primitive or boxed) in Java or of a type
    that can be converted to that. That means, one should be able to use this
    aggregator with ``int`` values sent from the Python client unless they
    are out of range for ``long`` type in Java.

    Args:
        attribute_path: Extracts values from this path, if given.

    Returns:
        An aggregator that calculates the average of the input values.
    """
    return _LongAverageAggregator(attribute_path)


def long_sum(attribute_path: str = None) -> Aggregator[int]:
    """Creates an aggregator that calculates the sum of the input values.

    Does NOT accept ``None`` input values or ``None`` extracted values.

    Since the server-side implementation is in Java, values stored in the
    Map must be of type ``long`` (primitive or boxed) in Java or of a type
    that can be converted to that. That means, one should be able to use this
    aggregator with ``int`` values sent from the Python client unless they
    are out of range for ``long`` type in Java.

    Args:
        attribute_path: Extracts values from this path, if given.

    Returns:
        An aggregator that calculates the sum of the input values.
    """
    return _LongSumAggregator(attribute_path)


def max_(attribute_path: str = None) -> Aggregator[AggregatorResultType]:
    """Creates an aggregator that calculates the max of the input values.

    Accepts ``None`` input values and ``None`` extracted values.

    Since the server-side implementation is in Java, values stored in the
    Map must implement the ``Comparable`` interface in Java. That means, one
    should be able to use this aggregator with most of the primitive values
    sent from the Python client, as Java implements this interface for the
    equivalents of types like ``int``, ``str``, and ``float``.

    Args:
        attribute_path: Extracts values from this path, if given.

    Returns:
        An aggregator that calculates the max of the input values.
    """
    return _MaxAggregator(attribute_path)


def min_(attribute_path: str = None) -> Aggregator[AggregatorResultType]:
    """Creates an aggregator that calculates the min of the input values.

    Accepts ``None`` input values and ``None`` extracted values.

    Since the server-side implementation is in Java, values stored in the
    Map must implement the ``Comparable`` interface in Java. That means, one
    should be able to use this aggregator with most of the primitive values
    sent from the Python client, as Java implements this interface for the
    equivalents of types like ``int``, ``str``, and ``float``.

    Args:
        attribute_path: Extracts values from this path, if given.

    Returns:
        An aggregator that calculates the min of the input values.
    """
    return _MinAggregator(attribute_path)


def number_avg(attribute_path: str = None) -> Aggregator[float]:
    """Creates an aggregator that calculates the average of the input values.

    Does NOT accept ``None`` input values or ``None`` extracted values.

    Accepts generic number input values. That means, one should be able to
    use this aggregator with ``float`` or ``int`` value sent from the Python
    client unless they are out of range for ``double`` type in Java.

    Args:
        attribute_path: Extracts values from this path, if given.

    Returns:
        An aggregator that calculates the average of the input values.
    """
    return _NumberAverageAggregator(attribute_path)


def max_by(attribute_path: str) -> Aggregator[MapEntry[KeyType, ValueType]]:
    """Creates an aggregator that calculates the max of the input values
    extracted from the given ``attribute_path`` and returns the input
    item containing the maximum value. If multiple items contain the
    maximum value, any of them is returned.

    Accepts ``None`` input values and ``None`` extracted values.

    Since the server-side implementation is in Java, values stored in the
    Map must implement the ``Comparable`` interface in Java. That means, one
    should be able to use this aggregator with most of the primitive values
    sent from the Python client, as Java implements this interface for the
    equivalents of types like ``int``, ``str``, and ``float``.

    Args:
        attribute_path: Path to extract values from.

    Returns:
        An aggregator that calculates the input value containing the maximum
        value extracted from the path.
    """
    return _MaxByAggregator(attribute_path)


def min_by(attribute_path: str) -> Aggregator[MapEntry[KeyType, ValueType]]:
    """Creates an aggregator that calculates the min of the input values
    extracted from the given ``attribute_path`` and returns the input
    item containing the minimum value. If multiple items contain the
    minimum value, any of them is returned.

    Accepts ``None`` input values and ``None`` extracted values.

    Since the server-side implementation is in Java, values stored in the
    Map must implement the ``Comparable`` interface in Java. That means, one
    should be able to use this aggregator with most of the primitive values
    sent from the Python client, as Java implements this interface for the
    equivalents of types like ``int``, ``str``, and ``float``.

    Args:
        attribute_path: Path to extract values from.

    Returns:
        An aggregator that calculates the input value containing the minimum
        value extracted from the path.
    """
    return _MinByAggregator(attribute_path)
