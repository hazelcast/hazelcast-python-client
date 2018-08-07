from hazelcast.serialization.api import IdentifiedDataSerializable

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

