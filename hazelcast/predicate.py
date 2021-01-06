from hazelcast.serialization.api import IdentifiedDataSerializable
from hazelcast.util import IterationType, get_attr_name

PREDICATE_FACTORY_ID = -20


class Predicate(object):
    """Represents a map entry predicate. Implementations of this class are
    basic building blocks for performing queries on map entries.

    **Special Attributes**

    The predicates that accept an attribute name support two special
    attributes:

    - ``__key`` - instructs the predicate to act on the key associated
      with an item.
    - ``this`` - instructs the predicate to act on the value associated
      with an item.

    **Attribute Paths**

    Dot notation may be used for attribute name to instruct the predicate to
    act on the attribute located at deeper level of an item. Given
    ``"full_name.first_name"`` path the predicate will act on ``first_name``
    attribute of the value fetched by ``full_name`` attribute from the item
    itself. If any of the attributes along the path can't be resolved,
    ``IllegalArgumentError`` will be thrown. Reading of any attribute from
    ``None`` will produce ``None`` value.

    Square brackets notation may be used to instruct the predicate to act on
    the list element at the specified index. Given ``"names[0]"`` path the
    predicate will act on the first item of the list fetched by ``names``
    attribute from the item. The index must be non-negative, otherwise
    ``IllegalArgumentError`` will be thrown. Reading from the index pointing
    beyond the end of the list will produce ``None`` value.

    Special ``any`` keyword may be used to act on every list element. Given
    ``"names[any].full_name.first_name"`` path the predicate will act on
    ``first_name`` attribute of the value fetched by ``full_name`` attribute
    from every list element stored in the item itself under ``names``
    attribute.

    **Handling of None**

    The predicates that accept ``None`` as a value to compare with or a pattern
    to match against if and only if that is explicitly stated in the method
    documentation. In this case, the usual equality logic applies: if ``None``
    is provided, the predicate passes an item if and only if the value stored
    under the item attribute in question is also ``None``.

    Special care must be taken while comparing with ``None`` values *stored*
    inside items being filtered through the predicates created by the following
    methods: :func:`greater`, :func:`greater_or_equal`, :func:`less`,
    :func:`less_or_equal`, :func:`between`. They always evaluate to ``False``
    and therefore never pass such items.

    **Implicit Type Conversion**

    If the type of the stored value doesn't match the type of the value
    provided to the predicate, implicit type conversion is performed before
    predicate evaluation. The provided value is converted to match the type of
    the stored attribute value. If no conversion matching the type exists,
    ``IllegalArgumentError`` is thrown.
    """

    pass


class PagingPredicate(Predicate):
    """This class is a special Predicate which helps to get a page-by-page
    result of a query.

    It can be constructed with a page-size, an inner predicate for filtering,
    and a comparator for sorting. This class is not thread-safe and stateless.
    To be able to reuse for another query, one should call :func:`reset`.
    """

    def reset(self):
        """Resets the predicate for reuse."""
        raise NotImplementedError("reset")

    def next_page(self):
        """Sets page index to next page.

        If new index is out of range, the query results that this
        paging predicate will retrieve will be an empty list.

        Returns:
            int: Updated page index
        """
        raise NotImplementedError("next_page")

    def previous_page(self):
        """Sets page index to previous page.

        If current page index is 0, this method does nothing.

        Returns:
            int: Updated page index.
        """
        raise NotImplementedError("previous_page")

    @property
    def page(self):
        """The current page index.


        :getter: Returns the current page index.
        :setter: Sets the current page index. If the page is out of range, the
            query results that this paging predicate will retrieve will be an
            empty list. New page index must be greater than or equal to ``0``.
        :type: int
        """
        raise NotImplementedError("page")

    @page.setter
    def page(self, new_page):
        raise NotImplementedError("page.setter")

    @property
    def page_size(self):
        """The page size.

        :getter: Returns the page size.
        :type: int
        """
        raise NotImplementedError("page_size")


class _AbstractPredicate(IdentifiedDataSerializable, Predicate):
    def write_data(self, object_data_output):
        raise NotImplementedError("write_data")

    def read_data(self, object_data_input):
        pass

    def get_factory_id(self):
        return PREDICATE_FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID


class _SqlPredicate(_AbstractPredicate):
    CLASS_ID = 0

    def __init__(self, expression=None):
        self.sql = expression

    def write_data(self, output):
        output.write_utf(self.sql)

    def __repr__(self):
        return "SqlPredicate(sql='%s')" % self.sql


class _AndPredicate(_AbstractPredicate):
    CLASS_ID = 1

    def __init__(self, predicates):
        self.predicates = predicates

    def write_data(self, output):
        output.write_int(len(self.predicates))
        for predicate in self.predicates:
            output.write_object(predicate)

    def __repr__(self):
        return "AndPredicate(%s)" % ", ".join([str(p) for p in self.predicates])


class _BetweenPredicate(_AbstractPredicate):
    CLASS_ID = 2

    def __init__(self, attribute, from_, to):
        self.attribute = attribute
        self.from_ = from_
        self.to = to

    def write_data(self, output):
        output.write_utf(self.attribute)
        output.write_object(self.to)
        output.write_object(self.from_)

    def __repr__(self):
        return "BetweenPredicate(attribute='%s', from=%s, to=%s)" % (
            self.attribute,
            self.from_,
            self.to,
        )


class _EqualPredicate(_AbstractPredicate):
    CLASS_ID = 3

    def __init__(self, attribute, value):
        self.attribute = attribute
        self.value = value

    def write_data(self, output):
        output.write_utf(self.attribute)
        output.write_object(self.value)

    def __repr__(self):
        return "EqualPredicate(attribute='%s', value=%s)" % (self.attribute, self.value)


class _GreaterLessPredicate(_AbstractPredicate):
    CLASS_ID = 4

    def __init__(self, attribute, value, is_equal, is_less):
        self.attribute = attribute
        self.value = value
        self.is_equal = is_equal
        self.is_less = is_less

    def write_data(self, output):
        output.write_utf(self.attribute)
        output.write_object(self.value)
        output.write_boolean(self.is_equal)
        output.write_boolean(self.is_less)

    def __repr__(self):
        return "GreaterLessPredicate(attribute='%s', value=%s, is_equal=%s, is_less=%s)" % (
            self.attribute,
            self.value,
            self.is_equal,
            self.is_less,
        )


class _LikePredicate(_AbstractPredicate):
    CLASS_ID = 5

    def __init__(self, attribute, pattern):
        self.attribute = attribute
        self.expression = pattern

    def write_data(self, output):
        output.write_utf(self.attribute)
        output.write_utf(self.expression)

    def __repr__(self):
        return "LikePredicate(attribute='%s', expression='%s')" % (self.attribute, self.expression)


class _ILikePredicate(_LikePredicate):
    CLASS_ID = 6

    def __repr__(self):
        return "ILikePredicate(attribute='%s', expression='%s')" % (self.attribute, self.expression)


class _InPredicate(_AbstractPredicate):
    CLASS_ID = 7

    def __init__(self, attribute, values):
        self.attribute = attribute
        self.values = values

    def write_data(self, output):
        output.write_utf(self.attribute)
        output.write_int(len(self.values))
        for value in self.values:
            output.write_object(value)

    def __repr__(self):
        return "InPredicate(attribute='%s', %s)" % (
            self.attribute,
            ",".join([str(x) for x in self.values]),
        )


class _InstanceOfPredicate(_AbstractPredicate):
    CLASS_ID = 8

    def __init__(self, class_name):
        self.class_name = class_name

    def write_data(self, output):
        output.write_utf(self.class_name)

    def __repr__(self):
        return "InstanceOfPredicate(class_name='%s')" % self.class_name


class _NotEqualPredicate(_EqualPredicate):
    CLASS_ID = 9

    def __repr__(self):
        return "NotEqualPredicate(attribute='%s', value=%s)" % (self.attribute, self.value)


class _NotPredicate(_AbstractPredicate):
    CLASS_ID = 10

    def __init__(self, predicate):
        self.predicate = predicate

    def write_data(self, output):
        output.write_object(self.predicate)

    def __repr__(self):
        return "NotPredicate(predicate=%s)" % self.predicate


class _OrPredicate(_AndPredicate):
    CLASS_ID = 11

    def __repr__(self):
        return "OrPredicate(%s)" % ", ".join([str(p) for p in self.predicates])


class _RegexPredicate(_AbstractPredicate):
    CLASS_ID = 12

    def __init__(self, attribute, pattern):
        self.attribute = attribute
        self.pattern = pattern

    def write_data(self, output):
        output.write_utf(self.attribute)
        output.write_utf(self.pattern)

    def __repr__(self):
        return "RegexPredicate(attribute='%s', pattern='%s')" % (self.attribute, self.pattern)


class _FalsePredicate(_AbstractPredicate):
    CLASS_ID = 13

    def write_data(self, object_data_output):
        pass

    def __repr__(self):
        return "FalsePredicate()"


class _TruePredicate(_AbstractPredicate):
    CLASS_ID = 14

    def write_data(self, object_data_output):
        pass

    def __repr__(self):
        return "TruePredicate()"


class _PagingPredicate(_AbstractPredicate, PagingPredicate):
    CLASS_ID = 15

    def __init__(self, predicate, page_size, comparator=None):
        if isinstance(predicate, PagingPredicate):
            raise TypeError("Nested paging predicate not supported.")

        if page_size <= 0:
            raise ValueError("page_size should be greater than 0.")

        self._internal_predicate = predicate
        self._page_size = page_size
        self._page = 0  # initialized to be on first page
        self.comparator = comparator
        self.iteration_type = IterationType.ENTRY
        self.anchor_list = []  # List of pairs: (nearest page, (anchor key, anchor value))

    def __repr__(self):
        return "PagingPredicate(predicate=%s, page_size=%s, comparator=%s)" % (
            self._internal_predicate,
            self.page_size,
            self.comparator,
        )

    def write_data(self, output):
        output.write_object(self._internal_predicate)
        output.write_object(self.comparator)
        output.write_int(self.page)
        output.write_int(self._page_size)
        output.write_utf(get_attr_name(IterationType, self.iteration_type))
        output.write_int(len(self.anchor_list))
        for nearest_page, (anchor_key, anchor_value) in self.anchor_list:
            output.write_int(nearest_page)
            output.write_object(anchor_key)
            output.write_object(anchor_value)

    def next_page(self):
        self.page += 1
        return self.page

    def previous_page(self):
        if self.page != 0:
            self.page -= 1
        return self.page

    def reset(self):
        self.iteration_type = IterationType.ENTRY
        del self.anchor_list[:]
        self.page = 0

    @property
    def page(self):
        return self._page

    @page.setter
    def page(self, new_page):
        if new_page < 0:
            raise ValueError("new_page should be positive or 0.")

        self._page = new_page

    @property
    def page_size(self):
        return self._page_size


def sql(expression):
    """Creates a predicate that will pass items that match the given SQL
    ``where`` expression.

    The following operators are supported: ``=``, ``<``, ``>``, ``<=``, ``>=``
    ``==``, ``!=``, ``<>``, ``BETWEEN``, ``IN``, ``LIKE``, ``ILIKE``, ``REGEX``
    ``AND``, ``OR``, ``NOT``.

    The operators are case-insensitive, but attribute names are case sensitive.

    Example: ``active AND (age > 20 OR salary < 60000)``

    Differences to standard SQL:

    - We don't use ternary boolean logic. ``field=10`` evaluates to ``false``,
      if ``field`` is ``null``, in standard SQL it evaluates to ``UNKNOWN``.
    - ``IS [NOT] NULL`` is not supported, use ``=NULL`` or ``<>NULL``.
    - ``IS [NOT] DISTINCT FROM`` is not supported, but ``=`` and ``<>`` behave
      like it.

    Args:
        expression (str): The ``where`` expression.

    Returns:
        Predicate: The created **sql** predicate instance.
    """
    return _SqlPredicate(expression)


def equal(attribute, value):
    """Creates a predicate that will pass items if the given ``value`` and the
    value stored under the given item ``attribute`` are equal.

    Args:
        attribute (str): The attribute to fetch the value for comparison from.
        value: The value to compare the attribute value against. Can be
            ``None``.

    Returns:
        Predicate: The created **equal** predicate instance.
    """
    return _EqualPredicate(attribute, value)


def not_equal(attribute, value):
    """Creates a predicate that will pass items if the given ``value`` and the
    value stored under the given item ``attribute`` are not equal.

    Args:
        attribute (str): The attribute to fetch the value for comparison from.
        value: The value to compare the attribute value against. Can be
            ``None``.

    Returns:
        Predicate: The created **not equal** predicate instance.
    """
    return _NotEqualPredicate(attribute, value)


def like(attribute, pattern):
    """Creates a predicate that will pass items if the given ``pattern``
    matches the value stored under the given item ``attribute``.

    Args:
        attribute (str): The attribute to fetch the value for matching from.
        pattern (str): The pattern to match the attribute value against.
            The ``%`` (percentage sign) is a placeholder for multiple
            characters, the ``_`` (underscore) is a placeholder for a single
            character. If you need to match the percentage sign or the
            underscore character itself, escape it with the backslash, for
            example ``"\\%"`` string will match the percentage sign. Can be
            ``None``.

    Returns:
        Predicate: The created **like** predicate instance.

    See Also:
        :func:`ilike` and :func:`regex`
    """
    return _LikePredicate(attribute, pattern)


def ilike(attribute, pattern):
    """Creates a predicate that will pass items if the given ``pattern``
    matches  the value stored under the given item ``attribute`` in a
    case-insensitive manner.

    Args:
        attribute (str): The attribute to fetch the value for matching from.
        pattern (str): The pattern to match the attribute value against.
            The ``%`` (percentage sign) is a placeholder for multiple
            characters, the ``_`` (underscore) is a placeholder for a single
            character. If you need to match the percentage sign or the
            underscore character itself, escape it with the backslash, for
            example ``"\\%"`` string will match the percentage sign. Can be
            ``None``.

    Returns:
        Predicate: The created **case-insensitive like** predicate instance.

    See Also:
        :func:`like` and :func:`regex`
    """
    return _ILikePredicate(attribute, pattern)


def regex(attribute, pattern):
    """Creates a predicate that will pass items if the given ``pattern``
    matches the value stored under the given item ``attribute``.

    Args:
        attribute (str): The attribute to fetch the value for matching from.
        pattern (str): The pattern to match the attribute value against. The
            pattern interpreted exactly the same as described in
            https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html.
            Can be ``None``.

    Returns:
        Predicate: The created **regex** predicate instance.

    See Also:
        :func:`ilike` and :func:`like`
    """
    return _RegexPredicate(attribute, pattern)


def and_(*predicates):
    """Creates a predicate that will perform the logical ``and`` operation on
    the given predicates.

    If no predicate is provided as argument, the created predicate will always
    evaluate to ``true`` and will pass any item.

    Args:
        *predicates (Predicate): The child predicates to form the resulting
            ``and`` predicate from.

    Returns:
        Predicate: The created **and** predicate instance.
    """
    return _AndPredicate(predicates)


def or_(*predicates):
    """Creates a predicate that will perform the logical ``or`` operation on
    the given predicates.

    If no predicate is provided as argument, the created predicate will always
    evaluate to ``false`` and will never pass any items.

    Args:
        *predicates (Predicate): The child predicates to form the resulting
            ``or`` predicate from.

    Returns:
        Predicate: The created **or** predicate instance.
    """
    return _OrPredicate(predicates)


def not_(predicate):
    """Creates a predicate that will negate the result of the given
    ``predicate``.

    Args:
        predicate (Predicate): The predicate to negate the value of.

    Returns:
        Predicate: The created **not** predicate instance.
    """
    return _NotPredicate(predicate)


def between(attribute, from_, to):
    """Creates a predicate that will pass items if the value stored under the
    given item ``attribute`` is contained inside the given range.

    The range begins at the given ``from_`` bound and ends at the given ``to``
    bound. The bounds are inclusive.

    Args:
        attribute (str): The attribute to fetch the value to check from.
        from_: The inclusive lower bound of the range to check.
        to: The inclusive upper bound of the range to check.

    Returns:
        Predicate: The created **between** predicate.
    """
    return _BetweenPredicate(attribute, from_, to)


def in_(attribute, *values):
    """Creates a predicate that will pass items if the value stored under the
    given item ``attribute`` is a member of the given ``values``.

    Args:
        attribute (str): The attribute to fetch the value to test from.
        *values: The values set to test the membership in. Individual values
            can be ``None``.

    Returns:
        Predicate: The created **in** predicate.
    """
    return _InPredicate(attribute, values)


def instance_of(class_name):
    """Creates a predicate that will pass entries for which the value class is
    an instance of the given ``class_name``.

    Args:
        class_name (str): The name of class the created predicate will check
            for.

    Returns:
        Predicate: The created **instance of** predicate.
    """
    return _InstanceOfPredicate(class_name)


def false():
    """Creates a predicate that will filter out all items.

    Returns:
        Predicate: The created **false** predicate.
    """
    return _FalsePredicate()


def true():
    """Creates a predicate that will pass all items.

    Returns:
        Predicate: The created **true** predicate.
    """
    return _TruePredicate()


def paging(predicate, page_size, comparator=None):
    """Creates a paging predicate with an inner predicate, page size and
    comparator. Results will be filtered via inner predicate and will be
    ordered via comparator if provided.

    Args:
        predicate (Predicate): The inner predicate through which results will
            be filtered. Can be ``None``. In that case, results will not be
            filtered.
        page_size (int): The page size.
        comparator (hazelcast.serialization.api.Portable or hazelcast.serialization.api.IdentifiedDataSerializable):
            The comparator through which results will be ordered. The
            comparision logic must be defined on the server side. Can be
            ``None``. In that case, the results will be returned in natural
            order.

    Returns:
        PagingPredicate: The created **paging** predicate.
    """
    return _PagingPredicate(predicate, page_size, comparator)


def greater(attribute, value):
    """
    Creates a predicate that will pass items if the value stored under the
    given item ``attribute`` is greater than the given ``value``.

    Args:
        attribute (str): The left-hand side attribute to fetch the value for
            comparison from.
        value: The right-hand side value to compare the attribute value
            against.

    Returns:
        Predicate: The created **greater than** predicate.
    """
    return _GreaterLessPredicate(attribute, value, False, False)


def greater_or_equal(attribute, value):
    """Creates a predicate that will pass items if the value stored under the
    given item ``attribute`` is greater than or equal to the given ``value``.

    Args:
        attribute (str): the left-hand side attribute to fetch the value for
            comparison from.
        value: The right-hand side value to compare the attribute value
            against.

    Returns:
        Predicate: The created **greater than or equal to** predicate.
    """
    return _GreaterLessPredicate(attribute, value, True, False)


def less(attribute, value):
    """Creates a predicate that will pass items if the value stored under the
    given item ``attribute`` is less than the given ``value``.

    Args:
        attribute (str): The left-hand side attribute to fetch the value for
            comparison from.
        value: The right-hand side value to compare the attribute value
            against.

    Returns:
        Predicate: The created **less than** predicate.
    """
    return _GreaterLessPredicate(attribute, value, False, True)


def less_or_equal(attribute, value):
    """Creates a predicate that will pass items if the value stored under the
    given item ``attribute`` is less than or equal to the given ``value``.

    Args:
        attribute (str): The left-hand side attribute to fetch the value for
            comparison from.
        value: The right-hand side value to compare the attribute value
            against.

    Returns:
        Predicate: The created **less than or equal to** predicate.
    """
    return _GreaterLessPredicate(attribute, value, True, True)
