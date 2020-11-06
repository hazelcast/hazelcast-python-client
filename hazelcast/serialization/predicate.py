from hazelcast.serialization.api import IdentifiedDataSerializable
from hazelcast.util import IterationType, get_attr_name

PREDICATE_FACTORY_ID = -20


class Predicate(IdentifiedDataSerializable):
    def read_data(self, object_data_input):
        pass

    def get_factory_id(self):
        return PREDICATE_FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID


class SqlPredicate(Predicate):
    CLASS_ID = 0

    def __init__(self, sql=None):
        self.sql = sql

    def write_data(self, output):
        output.write_utf(self.sql)

    def __repr__(self):
        return "SqlPredicate(sql='%s')" % self.sql


class AndPredicate(Predicate):
    CLASS_ID = 1

    def __init__(self, *predicates):
        self.predicates = predicates

    def write_data(self, output):
        output.write_int(len(self.predicates))
        for predicate in self.predicates:
            output.write_object(predicate)

    def __repr__(self):
        return "AndPredicate(%s)" % ", ".join([str(p) for p in self.predicates])


class BetweenPredicate(Predicate):
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
        return "BetweenPredicate(attribute='%s', from=%s, to=%s)" % (self.attribute, self.from_, self.to)


class EqualPredicate(Predicate):
    CLASS_ID = 3

    def __init__(self, attribute, value):
        self.attribute = attribute
        self.value = value

    def write_data(self, output):
        output.write_utf(self.attribute)
        output.write_object(self.value)

    def __repr__(self):
        return "EqualPredicate(attribute='%s', value=%s)" % (self.attribute, self.value)


class GreaterLessPredicate(Predicate):
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
            self.attribute, self.value, self.is_equal, self.is_less)


class LikePredicate(Predicate):
    CLASS_ID = 5

    def __init__(self, attribute, expression):
        self.attribute = attribute
        self.expression = expression

    def write_data(self, output):
        output.write_utf(self.attribute)
        output.write_utf(self.expression)

    def __repr__(self):
        return "LikePredicate(attribute='%s', expression='%s')" % (self.attribute, self.expression)


class ILikePredicate(LikePredicate):
    CLASS_ID = 6

    def __repr__(self):
        return "ILikePredicate(attribute='%s', expression='%s')" % (self.attribute, self.expression)


class InPredicate(Predicate):
    CLASS_ID = 7

    def __init__(self, attribute, *values):
        self.attribute = attribute
        self.values = values

    def write_data(self, output):
        output.write_utf(self.attribute)
        output.write_int(len(self.values))
        for value in self.values:
            output.write_object(value)

    def __repr__(self):
        return "InPredicate(attribute='%s', %s)" % (self.attribute, ",".join([str(x) for x in self.values]))


class InstanceOfPredicate(Predicate):
    CLASS_ID = 8

    def __init__(self, class_name):
        self.class_name = class_name

    def write_data(self, output):
        output.write_utf(self.class_name)

    def __repr__(self):
        return "InstanceOfPredicate(class_name='%s')" % self.class_name


class NotEqualPredicate(EqualPredicate):
    CLASS_ID = 9

    def __repr__(self):
        return "NotEqualPredicate(attribute='%s', value=%s)" % (self.attribute, self.value)


class NotPredicate(Predicate):
    CLASS_ID = 10

    def __init__(self, predicate):
        self.predicate = predicate

    def write_data(self, output):
        output.write_object(self.predicate)

    def __repr__(self):
        return "NotPredicate(predicate=%s)" % self.predicate


class OrPredicate(AndPredicate):
    CLASS_ID = 11

    def __repr__(self):
        return "OrPredicate(%s)" % ", ".join([str(p) for p in self.predicates])


class RegexPredicate(Predicate):
    CLASS_ID = 12

    def __init__(self, attribute, pattern):
        self.attribute = attribute
        self.pattern = pattern

    def write_data(self, output):
        output.write_utf(self.attribute)
        output.write_utf(self.pattern)

    def __repr__(self):
        return "RegexPredicate(attribute='%s', pattern='%s')" % (self.attribute, self.pattern)


class FalsePredicate(Predicate):
    CLASS_ID = 13

    def write_data(self, object_data_output):
        pass

    def __repr__(self):
        return "FalsePredicate()"


class TruePredicate(Predicate):
    CLASS_ID = 14

    def write_data(self, object_data_output):
        pass

    def __repr__(self):
        return "TruePredicate()"


class PagingPredicate(Predicate):
    CLASS_ID = 15

    def __init__(self, predicate, page_size, comparator=None):
        """
        Creates a Paging predicate with provided arguments.

        Args:
            predicate (hazelcast.serialization.predicate.Predicate):
                Predicate to filter the results
            page_size (int): The page size of each result set.
            comparator (hazelcast.serialization.api.Portable or hazelcast.serialization.api.IdentifiedDataSerializable):
                An optional comparator object to be used on the server side
                to sort the results. It must be registered on the server side
                and implement the ``java.util.Comparator`` interface.
        """
        if isinstance(predicate, PagingPredicate):
            raise TypeError("Nested paging predicate not supported.")

        if page_size <= 0:
            raise ValueError('page_size should be greater than 0.')

        self._internal_predicate = predicate
        self._page_size = page_size
        self._page = 0  # initialized to be on first page
        self.comparator = comparator
        self.iteration_type = IterationType.ENTRY
        self.anchor_list = []  # List of pairs: (nearest page, (anchor key, anchor value))

    def __repr__(self):
        return "PagingPredicate(predicate=%s, page_size=%s, comparator=%s)" % (self._internal_predicate,
                                                                               self.page_size, self.comparator)

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
        """Sets page index to next page.

        If new index is out of range, the query results that this
        paging predicate will retrieve will be an empty list.

        Returns:
            int: Updated page index
        """
        self.page += 1
        return self.page

    def previous_page(self):
        """sets page index to previous page.

        If current page index is 0, this method does nothing.

        Returns:
            int: Updated page index.
        """
        if self.page != 0:
            self.page -= 1
        return self.page

    def reset(self):
        """Resets the predicate for reuse."""
        self.iteration_type = IterationType.ENTRY
        del self.anchor_list[:]
        self.page = 0

    @property
    def page(self):
        return self._page

    @page.setter
    def page(self, page_no):
        """Sets page index to specified page_no.

        If page_no is out of range, the query results that this paging predicate
        will retrieve will be an empty list.

        Args:
            page_no (int): New page index. Must be greater than or equal to ``0``.
        """
        if page_no < 0:
            raise ValueError("page_no should be positive or 0.")

        self._page = page_no

    @property
    def page_size(self):
        return self._page_size


sql = SqlPredicate
is_equal_to = EqualPredicate
is_not_equal_to = NotEqualPredicate
is_like = LikePredicate
is_ilike = ILikePredicate
matches_regex = RegexPredicate
and_ = AndPredicate
or_ = OrPredicate
not_ = NotPredicate
is_between = BetweenPredicate
is_in = InPredicate
is_instance_of = InstanceOfPredicate
is_not = NotPredicate
false = FalsePredicate
true = TruePredicate
paging = PagingPredicate


def is_greater_than(attribute, x):
    return GreaterLessPredicate(attribute, x, False, False)


def is_greater_than_or_equal_to(attribute, x):
    return GreaterLessPredicate(attribute, x, True, False)


def is_less_than(attribute, x):
    return GreaterLessPredicate(attribute, x, False, True)


def is_less_than_or_equal_to(attribute, x):
    return GreaterLessPredicate(attribute, x, True, True)

