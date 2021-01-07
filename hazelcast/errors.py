import socket

EXCEPTION_MESSAGE_TYPE = 0


def retryable(cls):
    cls.retryable = True
    return cls


class HazelcastError(Exception):
    """General HazelcastError class."""

    def __init__(self, message=None, cause=None):
        super(HazelcastError, self).__init__(message, cause)

    def __str__(self):
        message, cause = self.args
        if cause:
            return "%s\nCaused by: %s" % (message, str(cause))
        return message or self.__class__.__name__


class ArrayIndexOutOfBoundsError(HazelcastError):
    pass


class ArrayStoreError(HazelcastError):
    pass


class AuthenticationError(HazelcastError):
    pass


class CacheNotExistsError(HazelcastError):
    pass


@retryable
class CallerNotMemberError(HazelcastError):
    pass


class CancellationError(HazelcastError):
    pass


class ClassCastError(HazelcastError):
    pass


class ClassNotFoundError(HazelcastError):
    pass


class ConcurrentModificationError(HazelcastError):
    pass


class ConfigMismatchError(HazelcastError):
    pass


class ConfigurationError(HazelcastError):
    pass


class DistributedObjectDestroyedError(HazelcastError):
    pass


class DuplicateInstanceNameError(HazelcastError):
    pass


class HazelcastEOFError(HazelcastError):
    pass


class ExecutionError(HazelcastError):
    pass


@retryable
class HazelcastInstanceNotActiveError(HazelcastError):
    pass


class HazelcastOverloadError(HazelcastError):
    pass


class HazelcastSerializationError(HazelcastError):
    pass


class HazelcastIOError(HazelcastError):
    pass


class IllegalArgumentError(HazelcastError):
    pass


class IllegalAccessException(HazelcastError):
    pass


class IllegalAccessError(HazelcastError):
    pass


class IllegalMonitorStateError(HazelcastError):
    pass


class IllegalStateError(HazelcastError):
    pass


class IllegalThreadStateError(HazelcastError):
    pass


class IndexOutOfBoundsError(HazelcastError):
    pass


class HazelcastInterruptedError(HazelcastError):
    pass


class InvalidAddressError(HazelcastError):
    pass


class InvalidConfigurationError(HazelcastError):
    pass


@retryable
class MemberLeftError(HazelcastError):
    pass


class NegativeArraySizeError(HazelcastError):
    pass


class NoSuchElementError(HazelcastError):
    pass


class NotSerializableError(HazelcastError):
    pass


class NullPointerError(HazelcastError):
    pass


class OperationTimeoutError(HazelcastError):
    pass


@retryable
class PartitionMigratingError(HazelcastError):
    pass


class QueryError(HazelcastError):
    pass


class QueryResultSizeExceededError(HazelcastError):
    pass


class SplitBrainProtectionError(HazelcastError):
    pass


class ReachedMaxSizeError(HazelcastError):
    pass


class RejectedExecutionError(HazelcastError):
    pass


class ResponseAlreadySentError(HazelcastError):
    pass


@retryable
class RetryableHazelcastError(HazelcastError):
    pass


@retryable
class RetryableIOError(HazelcastError):
    pass


class HazelcastRuntimeError(HazelcastError):
    pass


class SecurityError(HazelcastError):
    pass


class SocketError(HazelcastError):
    pass


class StaleSequenceError(HazelcastError):
    pass


class TargetDisconnectedError(HazelcastError):
    pass


@retryable
class TargetNotMemberError(HazelcastError):
    pass


class HazelcastTimeoutError(HazelcastError):
    pass


class TopicOverloadError(HazelcastError):
    pass


class TransactionError(HazelcastError):
    pass


class TransactionNotActiveError(HazelcastError):
    pass


class TransactionTimedOutError(HazelcastError):
    pass


class URISyntaxError(HazelcastError):
    pass


class UTFDataFormatError(HazelcastError):
    pass


class UnsupportedOperationError(HazelcastError):
    pass


@retryable
class WrongTargetError(HazelcastError):
    pass


class XAError(HazelcastError):
    pass


class AccessControlError(HazelcastError):
    pass


class LoginError(HazelcastError):
    pass


class UnsupportedCallbackError(HazelcastError):
    pass


class NoDataMemberInClusterError(HazelcastError):
    pass


class ReplicatedMapCantBeCreatedOnLiteMemberError(HazelcastError):
    pass


class MaxMessageSizeExceededError(HazelcastError):
    pass


class WANReplicationQueueFullError(HazelcastError):
    pass


class HazelcastAssertionError(HazelcastError):
    pass


class OutOfMemoryError(HazelcastError):
    pass


class StackOverflowError(HazelcastError):
    pass


class NativeOutOfMemoryError(HazelcastError):
    pass


class ServiceNotFoundError(HazelcastError):
    pass


class StaleTaskIdError(HazelcastError):
    pass


class DuplicateTaskError(HazelcastError):
    pass


class StaleTaskError(HazelcastError):
    pass


class LocalMemberResetError(HazelcastError):
    pass


class IndeterminateOperationStateError(HazelcastError):
    pass


class NodeIdOutOfRangeError(HazelcastError):
    pass


@retryable
class TargetNotReplicaError(HazelcastError):
    pass


class MutationDisallowedError(HazelcastError):
    pass


class ConsistencyLostError(HazelcastError):
    pass


class HazelcastClientNotActiveError(HazelcastError):
    def __init__(self, message="Client is not active"):
        super(HazelcastClientNotActiveError, self).__init__(message)


class HazelcastCertificationError(HazelcastError):
    pass


class ClientOfflineError(HazelcastError):
    def __init__(self):
        super(ClientOfflineError, self).__init__("No connection found to cluster")


class ClientNotAllowedInClusterError(HazelcastError):
    pass


class VersionMismatchError(HazelcastError):
    pass


class NoSuchMethodError(HazelcastError):
    pass


class NoSuchMethodException(HazelcastError):
    pass


class NoSuchFieldError(HazelcastError):
    pass


class NoSuchFieldException(HazelcastError):
    pass


class NoClassDefFoundError(HazelcastError):
    pass


class UndefinedErrorCodeError(HazelcastError):
    pass


class SessionExpiredError(HazelcastError):
    pass


class WaitKeyCancelledError(HazelcastError):
    pass


class LockAcquireLimitReachedError(HazelcastError):
    pass


class LockOwnershipLostError(HazelcastError):
    pass


class CPGroupDestroyedError(HazelcastError):
    pass


@retryable
class CannotReplicateError(HazelcastError):
    pass


class LeaderDemotedError(HazelcastError):
    pass


class StaleAppendRequestError(HazelcastError):
    pass


class NotLeaderError(HazelcastError):
    pass


# Error Codes
_UNDEFINED = 0
_ARRAY_INDEX_OUT_OF_BOUNDS = 1
_ARRAY_STORE = 2
_AUTHENTICATION = 3
_CACHE = 4
_CACHE_LOADER = 5
_CACHE_NOT_EXISTS = 6
_CACHE_WRITER = 7
_CALLER_NOT_MEMBER = 8
_CANCELLATION = 9
_CLASS_CAST = 10
_CLASS_NOT_FOUND = 11
_CONCURRENT_MODIFICATION = 12
_CONFIG_MISMATCH = 13
_DISTRIBUTED_OBJECT_DESTROYED = 14
_EOF = 15
_ENTRY_PROCESSOR = 16
_EXECUTION = 17
_HAZELCAST = 18
_HAZELCAST_INSTANCE_NOT_ACTIVE = 19
_HAZELCAST_OVERLOAD = 20
_HAZELCAST_SERIALIZATION = 21
_IO = 22
_ILLEGAL_ARGUMENT = 23
_ILLEGAL_ACCESS_EXCEPTION = 24
_ILLEGAL_ACCESS_ERROR = 25
_ILLEGAL_MONITOR_STATE = 26
_ILLEGAL_STATE = 27
_ILLEGAL_THREAD_STATE = 28
_INDEX_OUT_OF_BOUNDS = 29
_INTERRUPTED = 30
_INVALID_ADDRESS = 31
_INVALID_CONFIGURATION = 32
_MEMBER_LEFT = 33
_NEGATIVE_ARRAY_SIZE = 34
_NO_SUCH_ELEMENT = 35
_NOT_SERIALIZABLE = 36
_NULL_POINTER = 37
_OPERATION_TIMEOUT = 38
_PARTITION_MIGRATING = 39
_QUERY = 40
_QUERY_RESULT_SIZE_EXCEEDED = 41
_SPLIT_BRAIN_PROTECTION = 42
_REACHED_MAX_SIZE = 43
_REJECTED_EXECUTION = 44
_RESPONSE_ALREADY_SENT = 45
_RETRYABLE_HAZELCAST = 46
_RETRYABLE_IO = 47
_RUNTIME = 48
_SECURITY = 49
_SOCKET = 50
_STALE_SEQUENCE = 51
_TARGET_DISCONNECTED = 52
_TARGET_NOT_MEMBER = 53
_TIMEOUT = 54
_TOPIC_OVERLOAD = 55
_TRANSACTION = 56
_TRANSACTION_NOT_ACTIVE = 57
_TRANSACTION_TIMED_OUT = 58
_URI_SYNTAX = 59
_UTF_DATA_FORMAT = 60
_UNSUPPORTED_OPERATION = 61
_WRONG_TARGET = 62
_XA = 63
_ACCESS_CONTROL = 64
_LOGIN = 65
_UNSUPPORTED_CALLBACK = 66
_NO_DATA_MEMBER = 67
_REPLICATED_MAP_CANT_BE_CREATED = 68
_MAX_MESSAGE_SIZE_EXCEEDED = 69
_WAN_REPLICATION_QUEUE_FULL = 70
_ASSERTION_ERROR = 71
_OUT_OF_MEMORY_ERROR = 72
_STACK_OVERFLOW_ERROR = 73
_NATIVE_OUT_OF_MEMORY_ERROR = 74
_SERVICE_NOT_FOUND = 75
_STALE_TASK_ID = 76
_DUPLICATE_TASK = 77
_STALE_TASK = 78
_LOCAL_MEMBER_RESET = 79
_INDETERMINATE_OPERATION_STATE = 80
_FLAKE_ID_NODE_ID_OUT_OF_RANGE_EXCEPTION = 81
_TARGET_NOT_REPLICA_EXCEPTION = 82
_MUTATION_DISALLOWED_EXCEPTION = 83
_CONSISTENCY_LOST_EXCEPTION = 84
_SESSION_EXPIRED_EXCEPTION = 85
_WAIT_KEY_CANCELLED_EXCEPTION = 86
_LOCK_ACQUIRE_LIMIT_REACHED_EXCEPTION = 87
_LOCK_OWNERSHIP_LOST_EXCEPTION = 88
_CP_GROUP_DESTROYED_EXCEPTION = 89
_CANNOT_REPLICATE_EXCEPTION = 90
_LEADER_DEMOTED_EXCEPTION = 91
_STALE_APPEND_REQUEST_EXCEPTION = 92
_NOT_LEADER_EXCEPTION = 93
_VERSION_MISMATCH_EXCEPTION = 94
_NO_SUCH_METHOD_ERROR = 95
_NO_SUCH_METHOD_EXCEPTION = 96
_NO_SUCH_FIELD_ERROR = 97
_NO_SUCH_FIELD_EXCEPTION = 98
_NO_CLASS_DEF_FOUND_ERROR = 99

_ERROR_CODE_TO_ERROR = {
    _ARRAY_INDEX_OUT_OF_BOUNDS: ArrayIndexOutOfBoundsError,
    _ARRAY_STORE: ArrayStoreError,
    _AUTHENTICATION: AuthenticationError,
    _CACHE_NOT_EXISTS: CacheNotExistsError,
    _CALLER_NOT_MEMBER: CallerNotMemberError,
    _CANCELLATION: CancellationError,
    _CLASS_CAST: ClassCastError,
    _CLASS_NOT_FOUND: ClassNotFoundError,
    _CONCURRENT_MODIFICATION: ConcurrentModificationError,
    _CONFIG_MISMATCH: ConfigMismatchError,
    _DISTRIBUTED_OBJECT_DESTROYED: DistributedObjectDestroyedError,
    _EOF: HazelcastEOFError,
    _EXECUTION: ExecutionError,
    _HAZELCAST: HazelcastError,
    _HAZELCAST_INSTANCE_NOT_ACTIVE: HazelcastInstanceNotActiveError,
    _HAZELCAST_OVERLOAD: HazelcastOverloadError,
    _HAZELCAST_SERIALIZATION: HazelcastSerializationError,
    _IO: HazelcastIOError,
    _ILLEGAL_ARGUMENT: IllegalArgumentError,
    _ILLEGAL_ACCESS_EXCEPTION: IllegalAccessException,
    _ILLEGAL_ACCESS_ERROR: IllegalAccessError,
    _ILLEGAL_MONITOR_STATE: IllegalMonitorStateError,
    _ILLEGAL_STATE: IllegalStateError,
    _ILLEGAL_THREAD_STATE: IllegalThreadStateError,
    _INDEX_OUT_OF_BOUNDS: IndexOutOfBoundsError,
    _INTERRUPTED: HazelcastInterruptedError,
    _INVALID_ADDRESS: InvalidAddressError,
    _INVALID_CONFIGURATION: InvalidConfigurationError,
    _MEMBER_LEFT: MemberLeftError,
    _NEGATIVE_ARRAY_SIZE: NegativeArraySizeError,
    _NO_SUCH_ELEMENT: NoSuchElementError,
    _NOT_SERIALIZABLE: NotSerializableError,
    _NULL_POINTER: NullPointerError,
    _OPERATION_TIMEOUT: OperationTimeoutError,
    _PARTITION_MIGRATING: PartitionMigratingError,
    _QUERY: QueryError,
    _QUERY_RESULT_SIZE_EXCEEDED: QueryResultSizeExceededError,
    _SPLIT_BRAIN_PROTECTION: SplitBrainProtectionError,
    _REACHED_MAX_SIZE: ReachedMaxSizeError,
    _REJECTED_EXECUTION: RejectedExecutionError,
    _RESPONSE_ALREADY_SENT: ResponseAlreadySentError,
    _RETRYABLE_HAZELCAST: RetryableHazelcastError,
    _RETRYABLE_IO: RetryableIOError,
    _RUNTIME: HazelcastRuntimeError,
    _SECURITY: SecurityError,
    _SOCKET: socket.error,
    _STALE_SEQUENCE: StaleSequenceError,
    _TARGET_DISCONNECTED: TargetDisconnectedError,
    _TARGET_NOT_MEMBER: TargetNotMemberError,
    _TIMEOUT: HazelcastTimeoutError,
    _TOPIC_OVERLOAD: TopicOverloadError,
    _TRANSACTION: TransactionError,
    _TRANSACTION_NOT_ACTIVE: TransactionNotActiveError,
    _TRANSACTION_TIMED_OUT: TransactionTimedOutError,
    _URI_SYNTAX: URISyntaxError,
    _UTF_DATA_FORMAT: UTFDataFormatError,
    _UNSUPPORTED_OPERATION: UnsupportedOperationError,
    _WRONG_TARGET: WrongTargetError,
    _XA: XAError,
    _ACCESS_CONTROL: AccessControlError,
    _LOGIN: LoginError,
    _UNSUPPORTED_CALLBACK: UnsupportedCallbackError,
    _NO_DATA_MEMBER: NoDataMemberInClusterError,
    _REPLICATED_MAP_CANT_BE_CREATED: ReplicatedMapCantBeCreatedOnLiteMemberError,
    _MAX_MESSAGE_SIZE_EXCEEDED: MaxMessageSizeExceededError,
    _WAN_REPLICATION_QUEUE_FULL: WANReplicationQueueFullError,
    _ASSERTION_ERROR: HazelcastAssertionError,
    _OUT_OF_MEMORY_ERROR: OutOfMemoryError,
    _STACK_OVERFLOW_ERROR: StackOverflowError,
    _NATIVE_OUT_OF_MEMORY_ERROR: NativeOutOfMemoryError,
    _SERVICE_NOT_FOUND: ServiceNotFoundError,
    _STALE_TASK_ID: StaleTaskIdError,
    _DUPLICATE_TASK: DuplicateTaskError,
    _STALE_TASK: StaleTaskError,
    _LOCAL_MEMBER_RESET: LocalMemberResetError,
    _INDETERMINATE_OPERATION_STATE: IndeterminateOperationStateError,
    _FLAKE_ID_NODE_ID_OUT_OF_RANGE_EXCEPTION: NodeIdOutOfRangeError,
    _TARGET_NOT_REPLICA_EXCEPTION: TargetNotReplicaError,
    _MUTATION_DISALLOWED_EXCEPTION: MutationDisallowedError,
    _CONSISTENCY_LOST_EXCEPTION: ConsistencyLostError,
    _SESSION_EXPIRED_EXCEPTION: SessionExpiredError,
    _WAIT_KEY_CANCELLED_EXCEPTION: WaitKeyCancelledError,
    _LOCK_ACQUIRE_LIMIT_REACHED_EXCEPTION: LockAcquireLimitReachedError,
    _LOCK_OWNERSHIP_LOST_EXCEPTION: LockOwnershipLostError,
    _CP_GROUP_DESTROYED_EXCEPTION: CPGroupDestroyedError,
    _CANNOT_REPLICATE_EXCEPTION: CannotReplicateError,
    _LEADER_DEMOTED_EXCEPTION: LeaderDemotedError,
    _STALE_APPEND_REQUEST_EXCEPTION: StaleAppendRequestError,
    _NOT_LEADER_EXCEPTION: NotLeaderError,
    _VERSION_MISMATCH_EXCEPTION: VersionMismatchError,
    _NO_SUCH_METHOD_ERROR: NoSuchMethodError,
    _NO_SUCH_METHOD_EXCEPTION: NoSuchMethodException,
    _NO_SUCH_FIELD_ERROR: NoSuchFieldError,
    _NO_SUCH_FIELD_EXCEPTION: NoSuchFieldException,
    _NO_CLASS_DEF_FOUND_ERROR: NoClassDefFoundError,
}

from hazelcast.protocol.builtin import ListMultiFrameCodec
from hazelcast.protocol.codec.custom.error_holder_codec import ErrorHolderCodec


class _ErrorsCodec(object):
    @staticmethod
    def decode(msg):
        msg.next_frame()
        return ListMultiFrameCodec.decode(msg, ErrorHolderCodec.decode)


def create_error_from_message(error_message):
    error_holders = _ErrorsCodec.decode(error_message)
    return _create_error(error_holders, 0)


def _create_error(error_holders, idx):
    if idx == len(error_holders):
        return None

    error_holder = error_holders[idx]
    error_class = _ERROR_CODE_TO_ERROR.get(error_holder.error_code, None)

    stack_trace = "\n".join(
        [
            "\tat %s.%s(%s:%s)" % (x.class_name, x.method_name, x.file_name, x.line_number)
            for x in error_holder.stack_trace_elements
        ]
    )
    message = "Exception from server: %s: %s\n %s" % (
        error_holder.class_name,
        error_holder.message,
        stack_trace,
    )
    if error_class:
        return error_class(message, _create_error(error_holders, idx + 1))
    else:
        return UndefinedErrorCodeError(message, error_holder.class_name)


def is_retryable_error(error):
    return hasattr(error, "retryable")
