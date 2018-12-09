from hazelcast.util import TimeUnit, calculate_version
from unittest import TestCase


class TimeUnitTest(TestCase):
    def test_nano_to_second(self):
        self.assertEqual(0.1, TimeUnit.to_seconds(0.1e9, TimeUnit.NANOSECOND))

    def test_micro_to_second(self):
        self.assertEqual(2, TimeUnit.to_seconds(2e6, TimeUnit.MICROSECOND))

    def test_milli_to_second(self):
        self.assertEqual(3, TimeUnit.to_seconds(3e3, TimeUnit.MILLISECOND))

    def test_second_to_second(self):
        self.assertEqual(5.5, TimeUnit.to_seconds(5.5, TimeUnit.SECOND))

    def test_minute_to_second(self):
        self.assertEqual(60, TimeUnit.to_seconds(1, TimeUnit.MINUTE))

    def test_hour_to_second(self):
        self.assertEqual(1800, TimeUnit.to_seconds(0.5, TimeUnit.HOUR))

    def test_numeric_string_to_second(self):
        self.assertEqual(1, TimeUnit.to_seconds("1000", TimeUnit.MILLISECOND))

    def test_unsupported_types_to_second(self):
        types = ["str", True, None, list(), set(), dict()]
        for type in types:
            with self.assertRaises((TypeError, ValueError)):
                TimeUnit.to_seconds(type, TimeUnit.SECOND)


class VersionUtilTest(TestCase):
    def test_version_string(self):
        self.assertEqual(-1, calculate_version(""))
        self.assertEqual(-1, calculate_version("a.3.7.5"))
        self.assertEqual(-1, calculate_version("3.a.5"))
        self.assertEqual(-1, calculate_version("3,7.5"))
        self.assertEqual(-1, calculate_version("3.7,5"))
        self.assertEqual(-1, calculate_version("10.99.RC1"))
        self.assertEqual(30702, calculate_version("3.7.2"))
        self.assertEqual(19930, calculate_version("1.99.30"))
        self.assertEqual(30700, calculate_version("3.7-SNAPSHOT"))
        self.assertEqual(30702, calculate_version("3.7.2-SNAPSHOT"))
        self.assertEqual(109902, calculate_version("10.99.2-SNAPSHOT"))
        self.assertEqual(109930, calculate_version("10.99.30-SNAPSHOT"))
        self.assertEqual(109900, calculate_version("10.99-RC1"))