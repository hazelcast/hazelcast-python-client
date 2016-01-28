from hazelcast.serialization.api import IdentifiedDataSerializable

PREDICATE_FACTORY_ID = -32


class SqlPredicate(IdentifiedDataSerializable):
    CLASS_ID = 0

    def __init__(self, sql=None):
        self.sql = sql

    def get_factory_id(self):
        return PREDICATE_FACTORY_ID

    def get_class_id(self):
        return self.CLASS_ID

    def read_data(self, object_data_input):
        self.sql = object_data_input.read_utf()

    def write_data(self, object_data_output):
        object_data_output.write_utf(self.sql)


class AndPredicate(IdentifiedDataSerializable):
    CLASS_ID = 1
    pass


class BetweenPredicate(IdentifiedDataSerializable):
    CLASS_ID = 2
    pass


class EqualPredicate(IdentifiedDataSerializable):
    CLASS_ID = 3
    pass


class GreaterLessPredicate(IdentifiedDataSerializable):
    CLASS_ID = 4
    pass


class LikePredicate(IdentifiedDataSerializable):
    CLASS_ID = 5


class ILikePredicate(IdentifiedDataSerializable):
    CLASS_ID = 6


class InPredicate(IdentifiedDataSerializable):
    CLASS_ID = 7


class InstanceOfPredicate(IdentifiedDataSerializable):
    CLASS_ID = 8


class NotEqualPredicate(IdentifiedDataSerializable):
    CLASS_ID = 9


class NotPredicate(IdentifiedDataSerializable):
    CLASS_ID = 10


class OrPredicate(IdentifiedDataSerializable):
    CLASS_ID = 11


class RegexPredicate(IdentifiedDataSerializable):
    CLASS_ID = 12


class FalsePredicate(IdentifiedDataSerializable):
    CLASS_ID = 13


class TruePredicate(IdentifiedDataSerializable):
    CLASS_ID = 14


FACTORY = {
    SqlPredicate.CLASS_ID: SqlPredicate,
    AndPredicate.CLASS_ID: AndPredicate,
    BetweenPredicate.CLASS_ID: BetweenPredicate,
    EqualPredicate.CLASS_ID: EqualPredicate,
    GreaterLessPredicate.CLASS_ID: GreaterLessPredicate,
    LikePredicate.CLASS_ID: LikePredicate,
    ILikePredicate.CLASS_ID: ILikePredicate,
    InPredicate.CLASS_ID: InPredicate,
    InstanceOfPredicate.CLASS_ID: InstanceOfPredicate,
    NotEqualPredicate.CLASS_ID: NotEqualPredicate,
    NotPredicate.CLASS_ID: NotPredicate,
    OrPredicate.CLASS_ID: OrPredicate,
    RegexPredicate.CLASS_ID: RegexPredicate,
    FalsePredicate.CLASS_ID: FalsePredicate,
    TruePredicate.CLASS_ID: TruePredicate
}
