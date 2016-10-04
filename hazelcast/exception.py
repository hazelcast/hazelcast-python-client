from hazelcast.protocol.error_codes import HAZELCAST_INSTANCE_NOT_ACTIVE, AUTHENTICATION, TARGET_DISCONNECTED, \
    TARGET_NOT_MEMBER, HAZELCAST_SERIALIZATION


def retryable(cls):
    """
    Makes the given error retryable.

    :param cls: (:class:`~hazelcast.exception.HazelcastError`), the given error.
    :return: (:class:`~hazelcast.exception.HazelcastError`), the given error with retryable property.
    """
    cls.retryable = True
    return cls


class HazelcastError(Exception):
    """
    General HazelcastError class.
    """
    pass


class AuthenticationError(HazelcastError):
    """
    A HazelcastError which is raised when there is an Authentication failure: e.g. credentials from client is not valid.
    """
    pass


@retryable
class HazelcastInstanceNotActiveError(HazelcastError):
    """
    A HazelcastError which is raised when HazelcastInstance is not active during an invocation.
    """
    pass


class HazelcastSerializationError(HazelcastError):
    """
    A HazelcastError which is raised when an error occurs while serializing/deserializing objects.
    """
    pass


@retryable
class TargetNotMemberError(HazelcastError):
    """
    A HazelcastError that indicates operation is send to a machine that isn't member of the cluster.
    """
    pass


@retryable
class TargetDisconnectedError(HazelcastError):
    """
    A HazelcastError that indicates that an operation is about to be send to a non existing machine.
    """
    pass


class TimeoutError(HazelcastError):
    """
    A HazelcastError which is raised when an operation times out.
    """
    pass


class TransactionError(HazelcastError):
    """
    A HazelcastError that is thrown when something goes wrong while dealing with transactions and transactional data
    structures.
    """
    pass


ERROR_CODE_TO_ERROR = {
    AUTHENTICATION: AuthenticationError,
    HAZELCAST_INSTANCE_NOT_ACTIVE: HazelcastInstanceNotActiveError,
    HAZELCAST_SERIALIZATION: HazelcastSerializationError,
    TARGET_DISCONNECTED: TargetDisconnectedError,
    TARGET_NOT_MEMBER: TargetNotMemberError
}


def create_exception(error_codec):
    """
    Creates an exception with given error codec.

    :param error_codec: (Error Codec), error codec which includes the class name, message and exception trace.
    :return: (Exception), the created exception.
    """
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
    """
    Determines whether the given error is retryable or not.
    :param error: (:class:`~hazelcast.exception.HazelcastError`), the given error.
    :return: (bool), ``true`` if the given error is retryable, ``false`` otherwise.
    """
    return hasattr(error, 'retryable')
