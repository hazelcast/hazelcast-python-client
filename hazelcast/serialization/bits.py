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

FMT_LE_INT = "<i"
FMT_LE_INT8 = "<b"
FMT_LE_UINT8 = "<B"
FMT_LE_INT16 = "<h"
FMT_LE_UINT16 = "<H"
FMT_LE_LONG = "<q"
FMT_LE_FLOAT = "<f"
FMT_LE_DOUBLE = "<d"

FMT_BE_INT = ">i"
FMT_BE_INT8 = ">b"
FMT_BE_UINT8 = ">B"
FMT_BE_INT16 = ">h"
FMT_BE_UINT16 = ">H"
FMT_BE_LONG = ">q"
FMT_BE_FLOAT = ">f"
FMT_BE_DOUBLE = ">d"

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


def calculate_size_str(val):
    return len(val) + INT_SIZE_IN_BYTES


def calculate_size_data(val):
    return len(val) + INT_SIZE_IN_BYTES


def calculate_size_address(val):
    return calculate_size_str(val.host) + INT_SIZE_IN_BYTES
