import struct

from hazelcast.serialization.api import *
from hazelcast.serialization.bits import *
from hazelcast.serialization.data import Data
from hazelcast import six
from hazelcast.six.moves import range



class _ObjectDataInput(ObjectDataInput):
    def __init__(self, buff, offset=0, serialization_service=None, is_big_endian=True):
        self._buffer = buff
        self._service = serialization_service
        self._is_big_endian = is_big_endian
        self._pos = offset
        self._size = len(buff)
        # Local cache struct formats according to endianness
        self._FMT_INT8 = FMT_BE_INT8 if self._is_big_endian else FMT_LE_INT8
        self._FMT_UINT8 = FMT_BE_UINT8 if self._is_big_endian else FMT_LE_UINT8
        self._FMT_INT = FMT_BE_INT if self._is_big_endian else FMT_LE_INT
        self._FMT_SHORT = FMT_BE_INT16 if self._is_big_endian else FMT_LE_INT16
        self._FMT_CHAR = FMT_BE_UINT16 if self._is_big_endian else FMT_LE_UINT16
        self._FMT_LONG = FMT_BE_LONG if self._is_big_endian else FMT_LE_LONG
        self._FMT_FLOAT = FMT_BE_FLOAT if self._is_big_endian else FMT_LE_FLOAT
        self._FMT_DOUBLE = FMT_BE_DOUBLE if self._is_big_endian else FMT_LE_DOUBLE

    def read_into(self, buff, offset=None, length=None):
        _off = offset if offset is not None else 0
        _len = length if length is not None else len(buff)
        if _off < 0 or _len < 0 or (_off + _len) > len(self._buffer):
            raise IndexError()
        elif length == 0:
            return
        if self._pos > self._size:
            raise IndexError()
        if self._pos + _len > self._size:
            _len = self._size - self._pos
        buff[_off: _off + _len] = self._buffer[self._pos:self._pos + _len]
        self._pos += _len

    def skip_bytes(self, count):
        raise NotImplementedError("skip_bytes not implemented!!!")

    def read_boolean(self, position=None):
        return self.read_byte(position) != 0

    def read_byte(self, position=None):
        self._check_available(self._pos, BYTE_SIZE_IN_BYTES)
        return self._read_from_buff(self._FMT_INT8, BYTE_SIZE_IN_BYTES, position)

    def read_unsigned_byte(self, position=None):
        self._check_available(self._pos, BYTE_SIZE_IN_BYTES)
        return self._read_from_buff(self._FMT_UINT8, BYTE_SIZE_IN_BYTES, position)

    def read_char(self, position=None):
        char_ord = self.read_short(position)
        return six.unichr(char_ord)

    def read_short(self, position=None):
        self._check_available(self._pos, SHORT_SIZE_IN_BYTES)
        return self._read_from_buff(self._FMT_SHORT, SHORT_SIZE_IN_BYTES, position)

    def read_unsigned_short(self, position=None):
        self._check_available(self._pos, SHORT_SIZE_IN_BYTES)
        return self._read_from_buff(self._FMT_CHAR, SHORT_SIZE_IN_BYTES, position)

    def read_int(self, position=None):
        self._check_available(position, INT_SIZE_IN_BYTES)
        return self._read_from_buff(self._FMT_INT, INT_SIZE_IN_BYTES, position)

    def read_long(self, position=None):
        self._check_available(self._pos, LONG_SIZE_IN_BYTES)
        return self._read_from_buff(self._FMT_LONG, LONG_SIZE_IN_BYTES, position)

    def read_float(self, position=None):
        self._check_available(self._pos, FLOAT_SIZE_IN_BYTES)
        return self._read_from_buff(self._FMT_FLOAT, FLOAT_SIZE_IN_BYTES, position)

    def read_double(self, position=None):
        self._check_available(self._pos, DOUBLE_SIZE_IN_BYTES)
        return self._read_from_buff(self._FMT_DOUBLE, DOUBLE_SIZE_IN_BYTES, position)

    def read_utf(self):
        length = self.read_int()
        if length == NULL_ARRAY_LENGTH:
            return None
        result = bytearray()
        for i in range(0, length):
            _first_byte = self.read_byte() & 0xFF
            b = _first_byte >> 4
            if 0 <= b <= 7:
                result.append(_first_byte)
                continue
            if 12 <= b <= 13:
                result.append(_first_byte)
                result.append(self.read_byte() & 0xFF)
                continue
            if b == 14:
                result.append(_first_byte)
                result.append(self.read_byte() & 0xFF)
                result.append(self.read_byte() & 0xFF)
                continue
            raise UnicodeDecodeError("Malformed utf-8 content")
        return result.decode("utf-8")

    def read_byte_array(self):
        length = self.read_int()
        if length == NULL_ARRAY_LENGTH:
            return None
        result = bytearray(length)
        if length > 0:
            self.read_into(result, 0, length)
        return [x for x in result]

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

    def read_utf_array(self):
        return self._read_array_fnc(self.read_utf)

    def read_object(self):
        return self._service.read_object(self)

    def read_data(self):
        buff = self.read_byte_array()
        return Data(buff) if buff is not None else None

    def is_big_endian(self):
        return self._is_big_endian

    def position(self):
        return self._pos

    def set_position(self, position):
        self._pos = position

    def size(self):
        return self._size

    # HELPERS
    def _check_available(self, position, size):
        _position = self._pos if position is None else position
        if _position < 0:
            raise ValueError
        if self._size - _position < size:
            raise EOFError("Cannot read {} bytes!".format(size))

    def _read_from_buff(self, fmt, size, position=None):
        if position is None:
            val = struct.unpack_from(fmt, self._buffer, self._pos)
            self._pos += size
        else:
            val = struct.unpack_from(fmt, self._buffer, position)
        return val[0]

    def _read_array_fnc(self, read_item_fnc):
        length = self.read_int()
        if length == NULL_ARRAY_LENGTH:
            return None
        if length > 0:
            return [read_item_fnc() for _ in range(0, length)]
        return []

    def __repr__(self):
        from binascii import hexlify
        buf = hexlify(self._buffer)
        pos_ = self._pos * 2
        return buf[:pos_] + "[" + buf[pos_] + "]" + buf[pos_ + 1:]
