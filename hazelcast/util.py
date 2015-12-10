import ctypes

def check_not_none(val, message):
    if val is None:
        raise AssertionError(message)


def copy_bytes_into(src_buf, dst_buf, offset, length):
    ctypes.memmove(ctypes.byref(dst_buf, offset), src_buf, length)