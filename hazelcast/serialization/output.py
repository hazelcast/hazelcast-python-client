import struct

from api import *
from bits import *


class ObjectDataOutput(ObjectDataOutput):
    def __init__(self, init_size, serialization_service, is_big_endian=True):
        self._init_size = init_size
        self._buffer = bytearray(init_size)
        self._service = serialization_service
        self._is_big_endian = is_big_endian
        self._pos = 0
        # Local cache struct formats according to endianness
        self._FMT_INT = FMT_BE_INT if self._is_big_endian else FMT_LE_INT
        self._FMT_SHORT = FMT_BE_INT16 if self._is_big_endian else FMT_LE_INT16
        self._FMT_CHAR = FMT_BE_UINT16 if self._is_big_endian else FMT_LE_UINT16
        self._FMT_LONG = FMT_BE_LONG if self._is_big_endian else FMT_LE_LONG
        self._FMT_FLOAT = FMT_BE_FLOAT if self._is_big_endian else FMT_LE_FLOAT
        self._FMT_DOUBLE = FMT_BE_DOUBLE if self._is_big_endian else FMT_LE_DOUBLE

    def __write(self, val):
        self.__ensure_available(BYTE_SIZE_IN_BYTES)
        self._buffer[self._pos] = val
        self._pos += BYTE_SIZE_IN_BYTES

    def write_from(self, buff, offset=None, length=None):
        _off = offset if offset is not None else 0
        _len = length if length is not None else len(buff)
        if _off < 0 or _len < 0 or (_off + _len) > len(buff):
            raise IndexError()
        elif length == 0:
            return
        self.__ensure_available()
        self._buffer[self._write_offset(): self._write_offset() + length] = buff[:]
        self._pos += _len

    def write_boolean(self, bool):
        self.__write(1 if bool else 0)

    def write_byte(self, val):
        self.__write(val)

    def write_short(self, val):
        struct.pack_into(self._FMT_INT, self._buffer, self._pos, val)
        self._pos += SHORT_SIZE_IN_BYTES

    def write_char(self, val):
        struct.pack_into(self._FMT_CHAR, self._buffer, self._pos, val)
        self._pos += CHAR_SIZE_IN_BYTES

    def write_int(self, val):
        struct.pack_into(self._FMT_INT, self._buffer, self._pos, val)
        self._pos += INT_SIZE_IN_BYTES

    def write_long(self, val):
        struct.pack_into(self._FMT_LONG, self._buffer, self._pos, val)
        self._pos += LONG_SIZE_IN_BYTES

    def write_float(self, val):
        struct.pack_into(self._FMT_FLOAT, self._buffer, self._pos, val)
        self._pos += FLOAT_SIZE_IN_BYTES

    def write_double(self, val):
        struct.pack_into(self._FMT_DOUBLE, self._buffer, self._pos, val)
        self._pos += DOUBLE_SIZE_IN_BYTES

    # def write_bytes(self, string):
    #     raise NotImplementedError()
    #
    # def write_chars(self, val):
    #     raise NotImplementedError()

    def write_utf(self, val):
        self.append_byte_array(val.encode("utf-8"))

    def write_byte_array(self, val):
        _len = len(val) if val is not None else NULL_ARRAY_LENGTH
        self.write_int(_len)
        if _len > 0:
            self.write_from(val)

    def write_boolean_array(self, val):
        self.__write_array_fnc(val, self.write_boolean)

    def write_char_array(self, val):
        self.__write_array_fnc(val, self.write_char)

    def write_int_array(self, val):
        self.__write_array_fnc(val, self.write_int)

    def write_long_array(self, val):
        self.__write_array_fnc(val, self.write_long)

    def write_double_array(self, val):
        self.__write_array_fnc(val, self.write_double)

    def write_float_array(self, val):
        self.__write_array_fnc(val, self.write_float)

    def write_short_array(self, val):
        self.__write_array_fnc(val, self.write_short)

    def write_utf_array(self, val):
        self.__write_array_fnc(val, self.write_utf)

    def write_object(self, val):
        self._service.write_object(self, val)

    def write_data(self, data):
        payload = data.to_bytes() if data is not None else None
        self.write_byte_array(payload)

    def to_byte_array(self):
        if self._buffer is None or self._pos == 0:
            return bytearray()
        new_buffer = bytearray(self._pos)
        new_buffer[:] = self._buffer[:]
        return new_buffer

    def is_big_endian(self):
        return self._is_big_endian

    # HELPERS
    def __write_array_fnc(self, val, item_write_fnc):
        _len = len(val) if val is not None else NULL_ARRAY_LENGTH
        self.write_int(_len)
        if _len > 0:
            for item in val:
                item_write_fnc(item)

    def __ensure_available(self, length):
        if self.__available() < length:
            if self._buffer is None:
                buffer_length = len(self._buffer)
                new_length = max(buffer_length << 1, buffer_length + length)
                new_buffer = bytearray(new_length)
                new_buffer[self._pos: self._pos + buffer_length] = self._buffer[:]
                self._buffer = new_buffer
            else:
                new_length = length * 2 if length > self._initialSize / 2 else self._initialSize
                self._buffer = bytearray(new_length)

    def __available(self):
        return len(self._buffer) if self._buffer is not None else 0
