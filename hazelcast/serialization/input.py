from hazelcast.serialization.api import *
from hazelcast.serialization.bits import *


class _ObjectDataInput(ObjectDataInput):
    __slots__ = (
        "_buffer",
        "_service",
        "_is_big_endian",
        "_pos",
        "_size",
        "_FMT_INT8",
        "_FMT_INT",
        "_FMT_SHORT",
        "_FMT_CHAR",
        "_FMT_LONG",
        "_FMT_FLOAT",
        "_FMT_DOUBLE",
    )

    def __init__(self, buff, offset=0, serialization_service=None, is_big_endian=True):
        self._buffer = buff
        self._service = serialization_service
        self._is_big_endian = is_big_endian
        self._pos = offset
        self._size = len(buff)
        # Local cache struct formats according to endianness
        self._FMT_INT8 = BE_INT8 if self._is_big_endian else LE_INT8
        self._FMT_INT = BE_INT if self._is_big_endian else LE_INT
        self._FMT_SHORT = BE_INT16 if self._is_big_endian else LE_INT16
        self._FMT_CHAR = BE_UINT16 if self._is_big_endian else LE_UINT16
        self._FMT_LONG = BE_LONG if self._is_big_endian else LE_LONG
        self._FMT_FLOAT = BE_FLOAT if self._is_big_endian else LE_FLOAT
        self._FMT_DOUBLE = BE_DOUBLE if self._is_big_endian else LE_DOUBLE

    def read_into(self, buff, offset=None, length=None):
        _off = offset if offset is not None else 0
        _len = length if length is not None else len(buff)
        if _off < 0 or _len < 0 or (_off + _len) > len(self._buffer):
            raise IndexError()
        elif _len == 0:
            return
        if self._pos > self._size:
            raise IndexError()
        if self._pos + _len > self._size:
            _len = self._size - self._pos
        buff[_off : _off + _len] = self._buffer[self._pos : self._pos + _len]
        self._pos += _len

    def skip_bytes(self, count):
        if count <= 0:
            return 0

        if self._pos + count > self._size:
            count = self._size - self._pos

        self._pos += count
        return count

    def read_boolean(self):
        return self.read_byte() != 0

    def read_boolean_positional(self, position: int) -> bool:
        return self.read_byte_positional(position) != 0

    def read_byte(self):
        self._check_available(self._pos, BYTE_SIZE_IN_BYTES)
        value = self._FMT_INT8.unpack_from(self._buffer, self._pos)[0]
        self._pos += BYTE_SIZE_IN_BYTES
        return value

    def read_byte_positional(self, position: int) -> int:
        self._check_available(position, BYTE_SIZE_IN_BYTES)
        return self._FMT_INT8.unpack_from(self._buffer, position)[0]

    def read_unsigned_byte(self):
        self._check_available(self._pos, BYTE_SIZE_IN_BYTES)
        value = self._buffer[self._pos]
        self._pos += BYTE_SIZE_IN_BYTES
        return value

    def read_char(self):
        char_ord = self.read_short()
        return chr(char_ord)

    def read_char_positional(self, position: int) -> str:
        char_ord = self.read_short_positional(position)
        return chr(char_ord)

    def read_short(self):
        self._check_available(self._pos, SHORT_SIZE_IN_BYTES)
        value = self._FMT_SHORT.unpack_from(self._buffer, self._pos)[0]
        self._pos += SHORT_SIZE_IN_BYTES
        return value

    def read_short_positional(self, position: int) -> int:
        self._check_available(position, SHORT_SIZE_IN_BYTES)
        return self._FMT_SHORT.unpack_from(self._buffer, position)[0]

    def read_unsigned_short(self):
        self._check_available(self._pos, SHORT_SIZE_IN_BYTES)
        value = self._FMT_CHAR.unpack_from(self._buffer, self._pos)[0]
        self._pos += SHORT_SIZE_IN_BYTES
        return value

    def read_int(self):
        self._check_available(self._pos, INT_SIZE_IN_BYTES)
        value = self._FMT_INT.unpack_from(self._buffer, self._pos)[0]
        self._pos += INT_SIZE_IN_BYTES
        return value

    def read_int_positional(self, position: int) -> int:
        self._check_available(position, INT_SIZE_IN_BYTES)
        return self._FMT_INT.unpack_from(self._buffer, position)[0]

    def read_long(self):
        self._check_available(self._pos, LONG_SIZE_IN_BYTES)
        value = self._FMT_LONG.unpack_from(self._buffer, self._pos)[0]
        self._pos += LONG_SIZE_IN_BYTES
        return value

    def read_long_positional(self, position: int) -> int:
        self._check_available(position, LONG_SIZE_IN_BYTES)
        return self._FMT_LONG.unpack_from(self._buffer, position)[0]

    def read_float(self):
        self._check_available(self._pos, FLOAT_SIZE_IN_BYTES)
        value = self._FMT_FLOAT.unpack_from(self._buffer, self._pos)[0]
        self._pos += FLOAT_SIZE_IN_BYTES
        return value

    def read_float_positional(self, position: int) -> float:
        self._check_available(position, FLOAT_SIZE_IN_BYTES)
        return self._FMT_FLOAT.unpack_from(self._buffer, position)[0]

    def read_double(self):
        self._check_available(self._pos, DOUBLE_SIZE_IN_BYTES)
        value = self._FMT_DOUBLE.unpack_from(self._buffer, self._pos)[0]
        self._pos += DOUBLE_SIZE_IN_BYTES
        return value

    def read_double_positional(self, position: int) -> float:
        self._check_available(position, DOUBLE_SIZE_IN_BYTES)
        return self._FMT_DOUBLE.unpack_from(self._buffer, position)[0]

    def read_string(self):
        length = self.read_int()
        if length == NULL_ARRAY_LENGTH:
            return None
        result = bytearray(length)
        if length > 0:
            self.read_into(result, 0, length)
        return result.decode("utf-8")

    def read_byte_array(self):
        length = self.read_int()
        if length == NULL_ARRAY_LENGTH:
            return None
        result = bytearray(length)
        if length > 0:
            self.read_into(result, 0, length)

        return result

    def read_i8_array(self) -> typing.List[int]:
        return self._read_array_fnc(self.read_byte)

    def read_boolean_array(self):
        return self._read_array_fnc(self.read_boolean)

    def read_char_array(self):
        return self._read_array_fnc(self.read_char)

    def read_int_array(self):
        return self._read_array_fnc(self.read_int)

    def read_long_array(self):
        return self._read_array_fnc(self.read_long)

    def read_double_array(self):
        return self._read_array_fnc(self.read_double)

    def read_float_array(self):
        return self._read_array_fnc(self.read_float)

    def read_short_array(self):
        return self._read_array_fnc(self.read_short)

    def read_string_array(self):
        return self._read_array_fnc(self.read_string)

    def read_object(self):
        return self._service.read_object(self)

    def is_big_endian(self):
        return self._is_big_endian

    def position(self):
        return self._pos

    def set_position(self, position):
        self._pos = position

    def size(self):
        return self._size

    def get_byte_order(self):
        if self._is_big_endian:
            return "BIG_ENDIAN"
        return "LITTLE_ENDIAN"

    def read_utf(self):
        return self.read_string()

    def read_utf_array(self):
        return self.read_string_array()

    # HELPERS
    def _check_available(self, position, size):
        if position < 0:
            raise ValueError
        if self._size - position < size:
            raise EOFError("Cannot read %s bytes!" % size)

    def _read_array_fnc(self, read_item_fnc):
        length = self.read_int()
        if length == NULL_ARRAY_LENGTH:
            return None
        return [read_item_fnc() for _ in range(length)]
