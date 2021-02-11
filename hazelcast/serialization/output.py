from hazelcast.serialization.api import *
from hazelcast.serialization.bits import *
from hazelcast.six.moves import range


class _ObjectDataOutput(ObjectDataOutput):
    def __init__(self, init_size, serialization_service, is_big_endian=True):
        self._init_size = init_size
        self._buffer = bytearray(init_size)
        self._service = serialization_service
        self._is_big_endian = is_big_endian
        self._pos = 0
        # Local cache struct formats according to endianness
        self._FMT_INT = BE_INT if self._is_big_endian else LE_INT
        self._FMT_SHORT = BE_INT16 if self._is_big_endian else LE_INT16
        self._CHAR_ENCODING = "utf_16_be" if self._is_big_endian else "utf_16_le"
        self._FMT_LONG = BE_LONG if self._is_big_endian else LE_LONG
        self._FMT_FLOAT = BE_FLOAT if self._is_big_endian else LE_FLOAT
        self._FMT_DOUBLE = BE_DOUBLE if self._is_big_endian else LE_DOUBLE

    def _write(self, val):
        self._ensure_available(BYTE_SIZE_IN_BYTES)
        self._buffer[self._pos] = val
        self._pos += BYTE_SIZE_IN_BYTES

    def write_from(self, buff, offset=None, length=None):
        _off = offset if offset is not None else 0
        _len = length if length is not None else len(buff)
        if _off < 0 or _len < 0 or (_off + _len) > len(buff):
            raise IndexError()
        elif length == 0:
            return
        self._ensure_available(_len)
        self._buffer[self._pos : self._pos + _len] = buff
        self._pos += _len

    def write_boolean(self, boolean):
        self._write(1 if boolean else 0)

    def write_byte(self, val):
        self._write(val)

    def write_short(self, val):
        self._ensure_available(SHORT_SIZE_IN_BYTES)
        self._FMT_SHORT.pack_into(self._buffer, self._pos, val)
        self._pos += SHORT_SIZE_IN_BYTES

    def write_char(self, val):
        encoded = val.encode(self._CHAR_ENCODING)
        self.write_from(encoded)

    def write_int(self, val, position=None):
        self._ensure_available(INT_SIZE_IN_BYTES)
        if position is None:
            self._FMT_INT.pack_into(self._buffer, self._pos, val)
            self._pos += INT_SIZE_IN_BYTES
        else:
            self._FMT_INT.pack_into(self._buffer, position, val)

    def write_int_big_endian(self, val):
        self._ensure_available(INT_SIZE_IN_BYTES)
        BE_INT.pack_into(self._buffer, self._pos, val)
        self._pos += INT_SIZE_IN_BYTES

    def write_long(self, val):
        self._ensure_available(LONG_SIZE_IN_BYTES)
        self._FMT_LONG.pack_into(self._buffer, self._pos, val)
        self._pos += LONG_SIZE_IN_BYTES

    def write_float(self, val):
        self._ensure_available(FLOAT_SIZE_IN_BYTES)
        self._FMT_FLOAT.pack_into(self._buffer, self._pos, val)
        self._pos += FLOAT_SIZE_IN_BYTES

    def write_double(self, val):
        self._ensure_available(DOUBLE_SIZE_IN_BYTES)
        self._FMT_DOUBLE.pack_into(self._buffer, self._pos, val)
        self._pos += DOUBLE_SIZE_IN_BYTES

    def write_string(self, val):
        if val is None:
            self.write_int(NULL_ARRAY_LENGTH)
        else:
            encoded_data = val.encode("utf-8")
            self.write_int(len(encoded_data))
            self.write_from(encoded_data)

    def write_bytes(self, val):
        n = len(val)
        self._ensure_available(n)
        for b in val:
            self.write_byte(ord(b))

    def write_chars(self, val):
        n = len(val)
        self._ensure_available(n * CHAR_SIZE_IN_BYTES)
        for c in val:
            self.write_char(c)

    def write_byte_array(self, val):
        _len = len(val) if val is not None else NULL_ARRAY_LENGTH
        self.write_int(_len)
        if _len > 0:
            self.write_from(val)

    def write_boolean_array(self, val):
        self._write_array_fnc(val, self.write_boolean)

    def write_char_array(self, val):
        self._write_array_fnc(val, self.write_char)

    def write_int_array(self, val):
        self._write_array_fnc(val, self.write_int)

    def write_long_array(self, val):
        self._write_array_fnc(val, self.write_long)

    def write_double_array(self, val):
        self._write_array_fnc(val, self.write_double)

    def write_float_array(self, val):
        self._write_array_fnc(val, self.write_float)

    def write_short_array(self, val):
        self._write_array_fnc(val, self.write_short)

    def write_string_array(self, val):
        self._write_array_fnc(val, self.write_string)

    def write_object(self, val):
        self._service.write_object(self, val)

    def to_byte_array(self):
        return self._buffer[: self._pos]

    def get_byte_order(self):
        if self._is_big_endian:
            return "BIG_ENDIAN"
        return "LITTLE_ENDIAN"

    def is_big_endian(self):
        return self._is_big_endian

    def position(self):
        return self._pos

    def set_position(self, position):
        self._pos = position

    def write_zero_bytes(self, count):
        for _ in range(0, count):
            self._write(0)

    def write_utf(self, val):
        self.write_string(val)

    def write_utf_array(self, val):
        self.write_string_array(val)

    # HELPERS
    def _write_array_fnc(self, val, item_write_fnc):
        _len = len(val) if val is not None else NULL_ARRAY_LENGTH
        self.write_int(_len)
        if _len > 0:
            for item in val:
                item_write_fnc(item)

    def _ensure_available(self, length):
        if self._available() < length:
            buffer_length = len(self._buffer)
            new_length = max(buffer_length << 1, buffer_length + length)
            new_buffer = bytearray(new_length)
            new_buffer[: self._pos] = self._buffer[: self._pos]
            self._buffer = new_buffer

    def _available(self):
        return len(self._buffer) - self._pos

    def __repr__(self):
        from binascii import hexlify

        buf = hexlify(self._buffer)
        pos_ = self._pos * 2
        return buf[:pos_] + "[" + buf[pos_] + "]" + buf[pos_ + 1 :]


class EmptyObjectDataOutput(ObjectDataOutput):
    def write_string_array(self, val):
        pass

    def write_string(self, val):
        pass

    def write_short_array(self, val):
        pass

    def write_short(self, val):
        pass

    def write_object(self, val):
        pass

    def write_long_array(self, val):
        pass

    def write_long(self, val):
        pass

    def write_int_array(self, val):
        pass

    def write_int(self, val):
        pass

    def write_from(self, buff, offset=None, length=None):
        pass

    def write_float_array(self, val):
        pass

    def write_float(self, val):
        pass

    def write_double_array(self, val):
        pass

    def write_double(self, val):
        pass

    def write_chars(self, val):
        pass

    def write_char_array(self, val):
        pass

    def write_char(self, val):
        pass

    def write_bytes(self, string):
        pass

    def write_byte_array(self, val):
        pass

    def write_byte(self, val):
        pass

    def write_boolean_array(self, val):
        pass

    def write_boolean(self, val):
        pass

    def to_byte_array(self):
        raise NotImplementedError("to_byte_array not implemented")

    def get_byte_order(self):
        pass

    def write_utf(self, val):
        pass

    def write_utf_array(self, val):
        pass
