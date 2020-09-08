import struct

"""
Constants
"""
BYTE_SIZE_IN_BYTES = 1
BOOLEAN_SIZE_IN_BYTES = 1
SHORT_SIZE_IN_BYTES = 2
CHAR_SIZE_IN_BYTES = 2
INT_SIZE_IN_BYTES = 4
FLOAT_SIZE_IN_BYTES = 4
LONG_SIZE_IN_BYTES = 8
DOUBLE_SIZE_IN_BYTES = 8
UUID_SIZE_IN_BYTES = 17  # bool + long + long

LE_INT = struct.Struct("<i")
LE_UINT = struct.Struct("<I")
LE_INT8 = struct.Struct("<b")
LE_UINT8 = struct.Struct("<B")
LE_INT16 = struct.Struct("<h")
LE_UINT16 = struct.Struct("<H")
LE_LONG = struct.Struct("<q")
LE_ULONG = struct.Struct("<Q")
LE_FLOAT = struct.Struct("<f")
LE_DOUBLE = struct.Struct("<d")

BE_INT = struct.Struct(">i")
BE_INT8 = struct.Struct(">b")
BE_UINT8 = struct.Struct(">B")
BE_INT16 = struct.Struct(">h")
BE_UINT16 = struct.Struct(">H")
BE_LONG = struct.Struct(">q")
BE_FLOAT = struct.Struct(">f")
BE_DOUBLE = struct.Struct(">d")

BIG_ENDIAN = 2
LITTLE_ENDIAN = 1

NULL_ARRAY_LENGTH = -1

# LIMITS
MAX_BYTE = 127
MIN_BYTE = -128

MAX_SHORT = 2 ** 16 - 1
MIN_SHORT = -2 ** 15

MAX_INT = 2 ** 32 - 1
MIN_INT = -2 ** 31

MAX_LONG = 2 ** 64 - 1
MIN_LONG = -2 ** 63

MAX_FLOAT32 = 3.4028235e+38
MIN_FLOAT32 = 1.4e-45

UUID_MSB_SHIFT = 64
UUID_LSB_MASK = 0xFFFFFFFFFFFFFFFF
