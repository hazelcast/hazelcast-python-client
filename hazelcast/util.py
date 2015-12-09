def check_not_none(val, message):
    if val is None:
        raise AssertionError(message)