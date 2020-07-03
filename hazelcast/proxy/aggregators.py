from hazelcast.proxy.aggregator import CountAggregator,\
    FloatAverageAggregator,\
    FloatSumAggregator,\
    FixedPointSumAggregator,\
    FloatingPointSumAggregator, \
    SumAggregator, \
    MaxAggregator, \
    MinAggregator, \
    AverageAggregator, \
    MaxByAggregator, \
    MinByAggregator, \
    DistinctValuesAggregator


def count(attribute_path=None):
    """
    :param attribute_path: extracts values from this path if given
    :return: an aggregator that counts the input values extracted from the given attributePath.
    Accepts null input values and null extracted values.
    Aggregation result type Long.
    """
    return CountAggregator(attribute_path)


def float_avg(attribute_path=None):
    """
    :param attribute_path: extracts values from this path if given
    :return: an aggregator that calculates the average of the input values extracted from the given attributePath.
    Does NOT accept null input values nor null extracted values.
    Accepts only Double input values (primitive and boxed).
    Aggregation result type is Double.
    """
    return FloatAverageAggregator(attribute_path)


def float_sum(attribute_path=None):
    """
    :param attribute_path: extracts values from this path if given
    :return: an aggregator that calculates the sum of the input values extracted from the given attributePath.
     Does NOT accept null input values.
     Accepts only Double input values (primitive and boxed).
     Aggregation result type is Double.
    """
    return FloatSumAggregator(attribute_path)


def average(attribute_path=None):
    """
    :param attribute_path: extracts values from this path if given
    :return: an aggregator that calculates the average of the input values extracted from the given attributePath.
    Does NOT accept null input values nor null extracted values.
    Accepts generic Number input values.
    Aggregation result type is Double.
    """
    return AverageAggregator(attribute_path)


def fixed_point_sum(attribute_path=None):
    """
    :param attribute_path: extracts values from this path if given
    :return: an aggregator that calculates the sum of the input values.
    Does NOT accept null input values.
    Accepts generic Number input values.
    Aggregation result type is Long.
    """
    return FixedPointSumAggregator(attribute_path)


def floating_point_sum(attribute_path=None):
    """
    :param attribute_path: extracts values from this path if given
    :return: an aggregator that calculates the sum of the input values.
    Does NOT accept null input values.
    Accepts generic Number input values.
    Aggregation result type is number.
    """
    return FloatingPointSumAggregator(attribute_path)


def max(attribute_path=None):
    """
    :param attribute_path: extracts values from this path if given
    :return: an aggregator that calculates the max of the input values.
    Accepts null input values.
    """
    return MaxAggregator(attribute_path)


def min(attribute_path=None):
    """

    :param attribute_path: extracts values from this path if given
    :return: an aggregator that calculates the max of the input values.
    Accepts null input values.
    """
    return MinAggregator(attribute_path)


def sum(attribute_path=None):
    """
    :param attribute_path: extracts values from this path if given
    :return: an aggregator that calculates the sum of the input values.
    Does NOT accept null input values.
    Accepts only Integer input values (primitive and boxed).
    Aggregation result type is number.
    """
    return SumAggregator(attribute_path)


def max_by(attribute_path=None):
    """
    :param attribute_path: extracts values from this path if given
    :return: an aggregator that calculates the max of the input values extracted from the given attributePath
    and returns the input item containing the maximum value. If multiple items contain the maximum value,
    any of them is returned.
    Accepts null input values and null extracted values.
    Accepts generic Comparable input values.
    """
    return MaxByAggregator(attribute_path)


def min_by(attribute_path=None):
    """
    :param attribute_path: extracts values from this path if given
    :return: an aggregator that calculates the min of the input values extracted from the given attributePath
    and returns the input item containing the minimum value. If multiple items contain the minimum value,
    any of them is returned.
    Accepts null input values and null extracted values.
    Accepts generic Comparable input values.
    """
    return MinByAggregator(attribute_path)


def distinct_values(attribute_path=None):
    """
    :param attribute_path: extracts values from this path if given
    :return: an aggregator that calculates the distinct set of input values extracted from the given attributePath.
    Accepts null input values and null extracted values.
    Aggregation result type is a Set of R.
    """
    return DistinctValuesAggregator(attribute_path)



