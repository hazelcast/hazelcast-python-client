import unittest

from hazelcast.predicate import (
    sql,
    and_,
    equal,
    between,
    less_or_equal,
    like,
    ilike,
    in_,
    instance_of,
    not_equal,
    not_,
    or_,
    regex,
    true,
    false,
    paging,
)


class PredicateStrTest(unittest.TestCase):
    def test_sql(self):
        predicate = sql("this == 'value-1'")
        self.assertEqual(str(predicate), "SqlPredicate(sql='this == 'value-1'')")

    def test_and(self):
        predicate = and_(equal("this", "value-1"), equal("this", "value-2"))
        self.assertEqual(
            str(predicate),
            "AndPredicate(EqualPredicate(attribute='this', value=value-1),"
            " EqualPredicate(attribute='this', value=value-2))",
        )

    def test_between(self):
        predicate = between("this", 1, 20)
        self.assertEqual(str(predicate), "BetweenPredicate(attribute='this', from=1, to=20)")

    def test_equal_str(self):
        predicate = equal("this", "value-1")
        self.assertEqual(str(predicate), "EqualPredicate(attribute='this', value=value-1)")

    def test_greater_less(self):
        predicate = less_or_equal("this", 10)
        self.assertEqual(
            str(predicate),
            "GreaterLessPredicate(attribute='this', value=10, is_equal=True, is_less=True)",
        )

    def test_like(self):
        predicate = like("this", "a%")
        self.assertEqual(str(predicate), "LikePredicate(attribute='this', expression='a%')")

    def test_ilike(self):
        predicate = ilike("this", "a%")
        self.assertEqual(str(predicate), "ILikePredicate(attribute='this', expression='a%')")

    def test_in(self):
        predicate = in_("this", 1, 5, 7)
        self.assertEqual(str(predicate), "InPredicate(attribute='this', 1,5,7)")

    def test_instance_of(self):
        predicate = instance_of("java.lang.Boolean")
        self.assertEqual(str(predicate), "InstanceOfPredicate(class_name='java.lang.Boolean')")

    def test_not_equal(self):
        predicate = not_equal("this", "value-1")
        self.assertEqual(str(predicate), "NotEqualPredicate(attribute='this', value=value-1)")

    def test_not(self):
        predicate = not_(equal("this", "value-1"))
        self.assertEqual(
            str(predicate),
            "NotPredicate(predicate=EqualPredicate(attribute='this', value=value-1))",
        )

    def test_or(self):
        predicate = or_(equal("this", "value-1"), equal("this", "value-2"))
        self.assertEqual(
            str(predicate),
            "OrPredicate(EqualPredicate(attribute='this', value=value-1),"
            " EqualPredicate(attribute='this', value=value-2))",
        )

    def test_regex(self):
        predicate = regex("this", "c[ar].*")
        self.assertEqual(str(predicate), "RegexPredicate(attribute='this', pattern='c[ar].*')")

    def test_true(self):
        predicate = true()
        self.assertEqual(str(predicate), "TruePredicate()")

    def test_false(self):
        predicate = false()
        self.assertEqual(str(predicate), "FalsePredicate()")

    def test_paging(self):
        predicate = paging(true(), 5)
        self.assertEqual(
            str(predicate),
            "PagingPredicate(predicate=TruePredicate(), page_size=5, comparator=None)",
        )
