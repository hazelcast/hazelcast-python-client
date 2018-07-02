import unittest
from types import FunctionType

from hazelcast.serialization.api import ObjectDataOutput, ObjectDataInput, Portable, PortableReader, PortableWriter, \
    StreamSerializer, IdentifiedDataSerializable
from hazelcast import six
from hazelcast.six.moves import range


class APITestCase(unittest.TestCase):
    def test_api_func_raise_error(self):
        # This test make sure that all API functions raise NotImplementedError
        self._call_all_func(ObjectDataOutput)
        self._call_all_func(ObjectDataInput)
        self._call_all_func(Portable)
        self._call_all_func(PortableReader)
        self._call_all_func(PortableWriter)
        self._call_all_func(StreamSerializer)
        self._call_all_func(IdentifiedDataSerializable)

    def _call_all_func(self, class_type):
        for meth in class_type.__dict__.values():
            try:
                if isinstance(meth, FunctionType):
                    with self.assertRaises(NotImplementedError):
                        params = [i for i in range(0, six.get_function_code(meth).co_argcount)]
                        meth(*params)
            except TypeError as e:
                six.print_(e)
