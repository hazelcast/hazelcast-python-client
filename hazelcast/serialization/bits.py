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
FMT_LE_UINT8 = "<B"
FMT_LE_UINT16 = "<H"
FMT_LE_LONG = "<q"

FMT_BE_INT = ">i"
FMT_BE_UINT8 = ">B"
FMT_BE_UINT16 = ">H"
FMT_BE_LONG = ">q"

BIG_ENDIAN = 2
LITTLE_ENDIAN = 1


def calculate_size_str(val):
    return len(val) + INT_SIZE_IN_BYTES


def calculate_size_data(val):
    return len(val) + INT_SIZE_IN_BYTES


def calculate_size_address(val):
    return calculate_size_str(val.host) + INT_SIZE_IN_BYTES
