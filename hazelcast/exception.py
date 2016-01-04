from hazelcast.protocol.error_codes import HAZELCAST_INSTANCE_NOT_ACTIVE


def retryable(cls):
    cls.retryable = True
    return cls


class HazelcastError(Exception):
    pass


@retryable
class HazelcastInstanceNotActiveError(Exception):
    pass


ERROR_CODE_TO_ERROR = {
    HAZELCAST_INSTANCE_NOT_ACTIVE: HazelcastInstanceNotActiveError
}


def create_exception(error_codec):
    if error_codec.error_code in ERROR_CODE_TO_ERROR:
        return ERROR_CODE_TO_ERROR[error_codec.error_code](error_codec.message)
    return HazelcastError(error_codec.message)


def is_retryable_error(error):
    return hasattr(error, 'retryable')
