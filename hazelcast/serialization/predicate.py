from hazelcast.serialization.api import IdentifiedDataSerializable
from hazelcast.util import ITERATION_TYPE

PREDICATE_FACTORY_ID = -32


class Predicate(IdentifiedDataSerializable):
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

    __NULL_ANCHOR = (-1, None)

    def __init__(self, predicate, page_size, comparator=None):
        """
        Creates a Paging predicate with provided page size, internal predicate, and optional comparator.
        :param predicate: predicate to filter the results
        :param page_size: page size of each result set
        :param comparator: (Optional) a comparator object used to sort the results.
            Defines compare method on (K,V) tuples. Must be an implementation of hazelcast.core.Comparator
            WARNING: comparator must extend Comparator, and either IdentifiedDataSerializable or Portable.
        """
        if isinstance(predicate, PagingPredicate):
            raise TypeError('Nested paging predicate not supported.')
        self._internal_predicate = predicate
        self._comparator = comparator
        if page_size <= 0:
            raise ValueError('page_size should be greater than 0.')
        self._page_size = page_size
        self._page = 0  # initialized to be on first page
        self.iteration_type = ITERATION_TYPE.ENTRY  # ENTRY as default.
        self.anchor_list = []  # List of pairs: (nearest page, (anchor key, anchor value))

    def __repr__(self):
        return "PagingPredicate(predicate=%s, page_size=%s, comparator=%s)" % (self._internal_predicate,
                                                                               self.page_size, self.comparator)

    def write_data(self, output):
        output.write_object(self._internal_predicate)
        output.write_object(self.comparator)
        output.write_int(self.page)
        output.write_int(self._page_size)
        output.write_utf(ITERATION_TYPE.reverse.get(self.iteration_type, None))
        output.write_int(len(self.anchor_list))
        for nearest_page, (anchor_key, anchor_value) in self.anchor_list:
            output.write_int(nearest_page)
            output.write_object(anchor_key)
            output.write_object(anchor_value)

    def next_page(self):
        """
        Sets page index to next page. If new index is out of range, the query results that this paging predicate will
        retrieve will be an empty list.
        :return (int) current page index
        """
        self.page += 1
        return self.page

    def previous_page(self):
        """
        If current page index is 0, this method does nothing. Otherwise, it sets page index to previous page.
        :return (int) current page index
        """
        if self.page != 0:
            self.page -= 1
        return self.page

    def set_anchor(self, nearest_page, anchor):
        anchor_entry = (nearest_page, anchor)
        anchor_count = len(self.anchor_list)
        if nearest_page < anchor_count:
            self.anchor_list[nearest_page] = anchor_entry
        elif nearest_page == anchor_count:
            self.anchor_list.append(anchor_entry)
        else:
            raise IndexError('Anchor index is not correct, expected: ' + str(self.page) + 'found: ' + str(anchor_count))

    def reset(self):
        self.iteration_type = ITERATION_TYPE.ENTRY
        self.anchor_list.clear()
        self.page = 0

    def get_nearest_anchor_entry(self):
        """
        After each query, an anchor entry is set for that page.
        For the next query user may set an arbitrary page.
        For example: user queried first 5 pages which means first 5 anchor is available
        if the next query is for the 10th page then the nearest anchor belongs to page 5
        but if the next query is for the 3nd page then the nearest anchor belongs to page 2.
        :return nearest anchored entry for current page
        """
        anchor_count = len(self.anchor_list)
        if self.page == 0 or anchor_count == 0:
            return PagingPredicate.__NULL_ANCHOR
        return self.anchor_list[self.page - 1] if self.page < anchor_count else self.anchor_list[anchor_count - 1]

    @property
    def page(self):
        return self._page

    @page.setter
    def page(self, page_no):
        """
        Sets page index to specified page_no.
        If page_no is out of range, the query results that this paging predicate will retrieve will be an empty list.
        :param page_no: (int) greater than or equal to 0.
        """
        if page_no < 0:
            raise ValueError('page_no should be positive or 0.')
        self._page = page_no

    @property
    def page_size(self):
        return self._page_size

    @property
    def internal_predicate(self):
        return self._internal_predicate

    @property
    def comparator(self):
        return self._comparator


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


def is_greater_than(attribute, x):
    return GreaterLessPredicate(attribute, x, False, False)


def is_greater_than_or_equal_to(attribute, x):
    return GreaterLessPredicate(attribute, x, True, False)


def is_less_than(attribute, x):
    return GreaterLessPredicate(attribute, x, False, True)


def is_less_than_or_equal_to(attribute, x):
    return GreaterLessPredicate(attribute, x, True, True)


def paging_predicate(predicate, page_size):
    return PagingPredicate(predicate, page_size)

