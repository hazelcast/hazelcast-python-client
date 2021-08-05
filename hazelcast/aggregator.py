from hazelcast.serialization.api import IdentifiedDataSerializable


_AGGREGATORS_FACTORY_ID = -29


class Aggregator(object):
    """Marker base class for all aggregators.

    Aggregators allow computing a value of some function (e.g sum or max) over
    the stored map entries. The computation is performed in a fully distributed
    manner, so no data other than the computed value is transferred to the client,
    making the computation fast.
    """

    pass


class _AbstractAggregator(Aggregator, IdentifiedDataSerializable):
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


class _CountAggregator(_AbstractAggregator):
    def write_data(self, object_data_output):
        object_data_output.write_string(self._attribute_path)
        object_data_output.write_long(0)

    def get_class_id(self):
        return 4


class _DoubleAverageAggregator(_AbstractAggregator):
    def write_data(self, object_data_output):
        object_data_output.write_string(self._attribute_path)
        object_data_output.write_double(0)
        object_data_output.write_long(0)

    def get_class_id(self):
        return 6


class _DoubleSumAggregator(_AbstractAggregator):
    def write_data(self, object_data_output):
        object_data_output.write_string(self._attribute_path)
        object_data_output.write_double(0)

    def get_class_id(self):
        return 7


class _NumberAverageAggregator(_AbstractAggregator):
    def write_data(self, object_data_output):
        object_data_output.write_string(self._attribute_path)
        object_data_output.write_double(0)
        object_data_output.write_long(0)

    def get_class_id(self):
        return 16


class _FixedPointSumAggregator(_AbstractAggregator):
    def write_data(self, object_data_output):
        object_data_output.write_string(self._attribute_path)
        object_data_output.write_long(0)

    def get_class_id(self):
        return 8


class _FloatingPointSumAggregator(_AbstractAggregator):
    def write_data(self, object_data_output):
        object_data_output.write_string(self._attribute_path)
        object_data_output.write_double(0)

    def get_class_id(self):
        return 9


class _MaxAggregator(_AbstractAggregator):
    def write_data(self, object_data_output):
        object_data_output.write_string(self._attribute_path)
        object_data_output.write_object(None)

    def get_class_id(self):
        return 14


class _MinAggregator(_AbstractAggregator):
    def write_data(self, object_data_output):
        object_data_output.write_string(self._attribute_path)
        object_data_output.write_object(None)

    def get_class_id(self):
        return 15


class _IntegerAverageAggregator(_AbstractAggregator):
    def write_data(self, object_data_output):
        object_data_output.write_string(self._attribute_path)
        object_data_output.write_long(0)
        object_data_output.write_long(0)

    def get_class_id(self):
        return 10


class _IntegerSumAggregator(_AbstractAggregator):
    def write_data(self, object_data_output):
        object_data_output.write_string(self._attribute_path)
        object_data_output.write_long(0)

    def get_class_id(self):
        return 11


class _LongAverageAggregator(_AbstractAggregator):
    def write_data(self, object_data_output):
        object_data_output.write_string(self._attribute_path)
        object_data_output.write_long(0)
        object_data_output.write_long(0)

    def get_class_id(self):
        return 12


class _LongSumAggregator(_AbstractAggregator):
    def write_data(self, object_data_output):
        object_data_output.write_string(self._attribute_path)
        object_data_output.write_long(0)

    def get_class_id(self):
        return 13


def count(attribute_path=None):
    """Creates count aggregator.

    Accepts ``None`` input values and ``None`` extracted values.

    Args:
        attribute_path (str): extracts values from this path if given

    Returns:
        Aggregator[int]: an aggregator that counts the input values
    """
    return _CountAggregator(attribute_path)


def double_avg(attribute_path=None):
    """Creates double average aggregator.

    Does NOT accept null input values.
    Accepts only ``float`` input values.

    Args:
        attribute_path (str): extracts values from this path if given

    Returns:
        Aggregator[float]: an aggregator that calculates the average of
        the input values
    """
    return _DoubleAverageAggregator(attribute_path)


def double_sum(attribute_path=None):
    """Creates double sum aggregator.

    Does NOT accept null input values.
    Accepts only double input values (primitive and boxed).

    Args:
        attribute_path (str): extracts values from this path if given

    Returns:
        Aggregator[float]: an aggregator that calculates the sum of input the
        values
    """
    return _DoubleSumAggregator(attribute_path)


def number_avg(attribute_path=None):
    """Creates number average aggregator.

    Does NOT accept null input values.
    Accepts generic number input values.
    Aggregation result type is number.

    Args:
        attribute_path (str): extracts values from this path if given

    Returns:
        Aggregator[float]: an aggregator that calculates the average of
        the input values
    """
    return _NumberAverageAggregator(attribute_path)


def fixed_point_sum(attribute_path=None):
    """Creates fixed point sum aggregator.

    Does NOT accept null input values.
    Accepts generic number input values.

    Args:
        attribute_path (str): extracts values from this path if given

    Returns:
        Aggregator[int]: an aggregator that calculates the sum of the
        input values
    """
    return _FixedPointSumAggregator(attribute_path)


def floating_point_sum(attribute_path=None):
    """Creates floating point sum aggregator.

    Does NOT accept null input values.
    Accepts generic number input values.

    Args:
        attribute_path (str): extracts values from this path if given

    Returns:
        Aggregator[float]: an aggregator that calculates the sum of the
        input values
    """
    return _FloatingPointSumAggregator(attribute_path)


def max_(attribute_path=None):
    """Creates max aggregator.

    Accepts any input values.

    Args:
        attribute_path (str): extracts values from this path if given

    Returns:
        Aggregator[any]: an aggregator that calculates the max of the input
        values
    """
    return _MaxAggregator(attribute_path)


def min_(attribute_path=None):
    """Creates min aggregator.

    Accepts any input values.

    Args:
        attribute_path (str): extracts values from this path if given

    Returns:
        Aggregator[any]: an aggregator that calculates the min of the
        input values
    """
    return _MinAggregator(attribute_path)


def int_avg(attribute_path=None):
    """Creates int average aggregator.

    Does NOT accept null input values.
    Accepts only long input values (primitive and boxed).

    Args:
        attribute_path (str): extracts values from this path if given

    Returns:
        Aggregator[int]: an aggregator that calculates the average of the
        input values
    """
    return _IntegerAverageAggregator(attribute_path)


def int_sum(attribute_path=None):
    """Creates int sum aggregator.

    Does NOT accept null input values.
    Accepts only long input values (primitive and boxed).

    Args:
        attribute_path (str): extracts values from this path if given

    Returns:
        Aggregator[int]: an aggregator that calculates the sum of the
        input values
    """
    return _IntegerSumAggregator(attribute_path)


def long_avg(attribute_path=None):
    """Creates long average aggregator.

    Does NOT accept null input values.
    Accepts only long input values (primitive and boxed).

    Args:
        attribute_path (str): extracts values from this path if given

    Returns:
        Aggregator[int]: an aggregator that calculates the average of the
        input values
    """
    return _LongAverageAggregator(attribute_path)


def long_sum(attribute_path=None):
    """Creates long sum aggregator.

    Does NOT accept null input values.
    Accepts only long input values (primitive and boxed).

    Args:
        attribute_path (str): extracts values from this path if given

    Returns:
        Aggregator[int]: an aggregator that calculates the sum of the
        input values
    """
    return _LongSumAggregator(attribute_path)
