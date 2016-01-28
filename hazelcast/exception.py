from hazelcast.protocol.error_codes import HAZELCAST_INSTANCE_NOT_ACTIVE, AUTHENTICATION, TARGET_DISCONNECTED, \
    TARGET_NOT_MEMBER, HAZELCAST_SERIALIZATION


def retryable(cls):
    cls.retryable = True
    return cls


class HazelcastError(Exception):
    pass


class AuthenticationError(HazelcastError):
    pass


@retryable
class HazelcastInstanceNotActiveError(HazelcastError):
    pass


class HazelcastSerializationError(HazelcastError):
    pass


@retryable
class TargetNotMemberError(HazelcastError):
    pass


@retryable
class TargetDisconnectedError(HazelcastError):
    pass


class TimeoutError(HazelcastError):
    pass


class TransactionError(HazelcastError):
    pass


ERROR_CODE_TO_ERROR = {
    AUTHENTICATION: AuthenticationError,
    HAZELCAST_INSTANCE_NOT_ACTIVE: HazelcastInstanceNotActiveError,
    HAZELCAST_SERIALIZATION: HazelcastSerializationError,
    TARGET_DISCONNECTED: TargetDisconnectedError,
    TARGET_NOT_MEMBER: TargetNotMemberError
}


def create_exception(error_codec):
    if error_codec.error_code in ERROR_CODE_TO_ERROR:
        return ERROR_CODE_TO_ERROR[error_codec.error_code](error_codec.message)

    stack_trace = "\n".join(
        ["\tat %s.%s(%s:%s)" % (x.declaring_class, x.method_name, x.file_name, x.line_number) for x in
         error_codec.stack_trace])
    message = "Got exception from server:\n %s: %s\n %s" % (error_codec.class_name,
                                                            error_codec.message,
                                                            stack_trace)
    return HazelcastError(message)


def is_retryable_error(error):
    return hasattr(error, 'retryable')
