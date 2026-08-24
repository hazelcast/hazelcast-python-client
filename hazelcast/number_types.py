from typing import Self

from hazelcast.serialization import MIN_SHORT, MAX_SHORT, MIN_INT, MAX_INT, MIN_LONG, MAX_LONG
from hazelcast.serialization.bits import MIN_BYTE, MAX_BYTE

__all__ = "Int8", "Int16", "Int32", "Int64", "Float32", "Float64", "BigInt"



class Int8:
    """Int8 represents an 8-bit signed integer

    Corresponds to Java ``byte``
    """

    MIN_VALUE = MIN_BYTE
    MAX_VALUE = MAX_BYTE

    def __init__(self, value: int):
        if not (self.MIN_VALUE <= value <= self.MAX_VALUE):
            raise ValueError("{} value must be between {} and {}".format(
                self.__class__.__name__, self.MIN_VALUE, self.MAX_VALUE,
            ))
        self.value = value

    def __int__(self):
        return self.value

    def __repr__(self) -> str:
        return str(self.value)


class Int16:
    """Int16 represents a 16-bit signed integer

    Corresponds to Java ``short``.
    """

    MIN_VALUE = MIN_SHORT
    MAX_VALUE = MAX_SHORT

    def __init__(self, value: int):
        if not (self.MIN_VALUE <= value <= self.MAX_VALUE):
            raise ValueError("{} value must be between {} and {}".format(
                self.__class__.__name__, self.MIN_VALUE, self.MAX_VALUE,
            ))
        self.value = value

    def __int__(self):
        return self.value

    def __repr__(self) -> str:
        return str(self.value)


class Int32:
    """Int32 represents a 32-bit signed integer

    Corresponds to Java ``int``.
    """

    MIN_VALUE = MIN_INT
    MAX_VALUE = MAX_INT

    def __init__(self, value: int):
        if not (self.MIN_VALUE <= value <= self.MAX_VALUE):
            raise ValueError("{} value must be between {} and {}".format(
                self.__class__.__name__, self.MIN_VALUE, self.MAX_VALUE,
            ))
        self.value = value

    def __int__(self):
        return self.value

    def __repr__(self) -> str:
        return str(self.value)


class Int64:
    """Int64 represents a 64-bit signed integer

    Corresponds to Java ``long``.
    """

    MIN_VALUE = MIN_LONG
    MAX_VALUE = MAX_LONG

    def __init__(self, value: int):
        if not (self.MIN_VALUE <= value <= self.MAX_VALUE):
            raise ValueError("{} value must be between {} and {}".format(
                self.__class__.__name__, self.MIN_VALUE, self.MAX_VALUE,
            ))
        self.value = value

    def __int__(self):
        return self.value

    def __repr__(self) -> str:
        return str(self.value)


class BigInt:
    """BigInt represents a big integer

    Corresponds to Java ``java.math.BigInteger``.
    """

    def __init__(self, value: int):
        self.value = value

    def __int__(self):
        return self.value

    def __repr__(self) -> str:
        return str(self.value)


class Float32:
    """Float32 represents a 32-bit floating point number

    Corresponds to Java ``float``.
    """

    def __init__(self, value: float|int):
        self.value = float(value)

    def __float__(self):
        return self.value

    def __repr__(self) -> str:
        return str(self.value)


class Float64:
    """Float32 represents a 64-bit floating point number

    Corresponds to Java ``double``.
    """

    def __init__(self, value: float|int):
        self.value = float(value)

    def __float__(self):
        return self.value

    def __repr__(self) -> str:
        return str(self.value)
