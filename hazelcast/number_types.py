import struct
import sys

from hazelcast.serialization import MIN_SHORT, MAX_SHORT, MIN_INT, MAX_INT, MIN_LONG, MAX_LONG
from hazelcast.serialization.bits import MIN_BYTE, MAX_BYTE

__all__ = "Int8", "Int16", "Int32", "Int64", "Float32", "Float64", "BigInt"


class Integer:

    def __init_subclass__(
        cls, min_value: int | None = None, max_value: int | None = None, **kwargs
    ):
        cls.MIN_VALUE = min_value  # type: ignore[attr-defined]
        cls.MAX_VALUE = max_value  # type: ignore[attr-defined]
        super().__init_subclass__(**kwargs)

    def __init__(self, value: int):
        if self.MIN_VALUE is not None and self.MAX_VALUE is not None:  # type: ignore[attr-defined]
            if not (self.MIN_VALUE <= value <= self.MAX_VALUE):  # type: ignore[attr-defined]
                raise ValueError(
                    "{} value must be between {} and {}".format(
                        self.__class__.__name__,
                        self.MIN_VALUE,  # type: ignore[attr-defined]
                        self.MAX_VALUE,  # type: ignore[attr-defined]
                    )
                )
        self.value = value

    def __int__(self):
        return self.value

    def __repr__(self) -> str:
        return str(self.value)

    def __eq__(self, value: object, /) -> bool:
        if not isinstance(value, self.__class__):
            return False
        return self.value == value.value

    def __hash__(self) -> int:
        return self.value.__hash__()


class Int8(Integer, min_value=MIN_BYTE, max_value=MAX_BYTE):
    """Int8 represents an 8-bit signed integer

    Corresponds to Java ``byte``
    """


class Int16(Integer, min_value=MIN_SHORT, max_value=MAX_SHORT):
    """Int16 represents a 16-bit signed integer

    Corresponds to Java ``short``.
    """


class Int32(Integer, min_value=MIN_INT, max_value=MAX_INT):
    """Int32 represents a 32-bit signed integer

    Corresponds to Java ``int``.
    """

    def __eq__(self, value: object, /) -> bool:
        # special treatment, since int is stored as int32 by default
        if isinstance(value, self.__class__):
            return self.value == value.value
        if isinstance(value, int):
            return self.value == value
        return False

    def __hash__(self) -> int:
        return self.value.__hash__()


class Int64(Integer, min_value=MIN_LONG, max_value=MAX_LONG):
    """Int64 represents a 64-bit signed integer

    Corresponds to Java ``long``.
    """


class BigInt(Integer):
    """BigInt represents a big integer

    Corresponds to Java ``java.math.BigInteger``.
    """

    def __init__(self, value: int):
        super().__init__(value)


class Float:

    # Python 3.14 raises an OverflowError if the value doesn't fit into 32bit float.
    # Previous versions silently discard it.
    if (sys.version_info.major, sys.version_info.minor) >= (3, 14):
        def __init__(self, value: float | int):
            try:
                struct.pack("f", value)
            except OverflowError:
                raise ValueError(f"{value} does not fit into float32")

            self.value = float(value)

        def __float__(self):
            return self.value

        def __repr__(self) -> str:
            return str(self.value)

        def __eq__(self, value: object, /) -> bool:
            if not isinstance(value, self.__class__):
                return False
            return self.value == value.value

        def __hash__(self) -> int:
            return self.value.__hash__()
    else:
        def __init__(self, value: float | int):
            self.value = float(value)

        def __float__(self):
            return self.value

        def __repr__(self) -> str:
            return str(self.value)

        def __eq__(self, value: object, /) -> bool:
            if not isinstance(value, self.__class__):
                return False
            return self.value == value.value

        def __hash__(self) -> int:
            return self.value.__hash__()


class Float32(Float):
    """Float32 represents a 32-bit floating point number

    Corresponds to Java ``float``.
    """


class Float64(Float):
    """Float64 represents a 64-bit floating point number

    Corresponds to Java ``double``.
    """

    def __eq__(self, value: object, /) -> bool:
        # special treatment, since float is stored as float32 by default
        if isinstance(value, self.__class__):
            return self.value == value.value
        if isinstance(value, float):
            return self.value == value
        return False

    def __hash__(self) -> int:
        return self.value.__hash__()
