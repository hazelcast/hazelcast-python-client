from hazelcast.protocol.error_codes import *


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


class HazelcastIllegalStateError(HazelcastError):
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


class QuorumError(HazelcastError):
    pass


class ReachedMaxSizeError(HazelcastError):
    pass


class RejectedExecutionError(HazelcastError):
    pass


class RemoteMapReduceError(HazelcastError):
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


@retryable
class TargetDisconnectedError(HazelcastError):
    pass


@retryable
class TargetNotMemberError(HazelcastError):
    pass


class TimeoutError(HazelcastError):
    pass


class TopicOverloadError(HazelcastError):
    pass


class TopologyChangedError(HazelcastError):
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


class HazelcastClientNotActiveException(ValueError):
    pass


class HazelcastCertificationError(HazelcastError):
    pass


ERROR_CODE_TO_ERROR = {
    ARRAY_INDEX_OUT_OF_BOUNDS: ArrayIndexOutOfBoundsError,
    ARRAY_STORE: ArrayStoreError,
    AUTHENTICATION: AuthenticationError,
    CACHE_NOT_EXISTS: CacheNotExistsError,
    CALLER_NOT_MEMBER: CallerNotMemberError,
    CANCELLATION: CancellationError,
    CLASS_CAST: ClassCastError,
    CLASS_NOT_FOUND: ClassNotFoundError,
    CONCURRENT_MODIFICATION: ConcurrentModificationError,
    CONFIG_MISMATCH: ConfigMismatchError,
    CONFIGURATION: ConfigurationError,
    DISTRIBUTED_OBJECT_DESTROYED: DistributedObjectDestroyedError,
    DUPLICATE_INSTANCE_NAME: DuplicateInstanceNameError,
    EOF: HazelcastEOFError,
    EXECUTION: ExecutionError,
    HAZELCAST: HazelcastError,
    HAZELCAST_INSTANCE_NOT_ACTIVE: HazelcastInstanceNotActiveError,
    HAZELCAST_OVERLOAD: HazelcastOverloadError,
    HAZELCAST_SERIALIZATION: HazelcastSerializationError,
    IO: HazelcastIOError,
    ILLEGAL_ARGUMENT: IllegalArgumentError,
    ILLEGAL_ACCESS_EXCEPTION: IllegalAccessException,
    ILLEGAL_ACCESS_ERROR: IllegalAccessError,
    ILLEGAL_MONITOR_STATE: IllegalMonitorStateError,
    ILLEGAL_STATE: HazelcastIllegalStateError,
    ILLEGAL_THREAD_STATE: IllegalThreadStateError,
    INDEX_OUT_OF_BOUNDS: IndexOutOfBoundsError,
    INTERRUPTED: HazelcastInterruptedError,
    INVALID_ADDRESS: InvalidAddressError,
    INVALID_CONFIGURATION: InvalidConfigurationError,
    MEMBER_LEFT: MemberLeftError,
    NEGATIVE_ARRAY_SIZE: NegativeArraySizeError,
    NO_SUCH_ELEMENT: NoSuchElementError,
    NOT_SERIALIZABLE: NotSerializableError,
    NULL_POINTER: NullPointerError,
    OPERATION_TIMEOUT: OperationTimeoutError,
    PARTITION_MIGRATING: PartitionMigratingError,
    QUERY: QueryError,
    QUERY_RESULT_SIZE_EXCEEDED: QueryResultSizeExceededError,
    QUORUM: QuorumError,
    REACHED_MAX_SIZE: ReachedMaxSizeError,
    REJECTED_EXECUTION: RejectedExecutionError,
    REMOTE_MAP_REDUCE: RemoteMapReduceError,
    RESPONSE_ALREADY_SENT: ResponseAlreadySentError,
    RETRYABLE_HAZELCAST: RetryableHazelcastError,
    RETRYABLE_IO: RetryableIOError,
    RUNTIME: HazelcastRuntimeError,
    SECURITY: SecurityError,
    SOCKET: SocketError,
    STALE_SEQUENCE: StaleSequenceError,
    TARGET_DISCONNECTED: TargetDisconnectedError,
    TARGET_NOT_MEMBER: TargetNotMemberError,
    TIMEOUT: TimeoutError,
    TOPIC_OVERLOAD: TopicOverloadError,
    TOPOLOGY_CHANGED: TopologyChangedError,
    TRANSACTION: TransactionError,
    TRANSACTION_NOT_ACTIVE: TransactionNotActiveError,
    TRANSACTION_TIMED_OUT: TransactionTimedOutError,
    URI_SYNTAX: URISyntaxError,
    UTF_DATA_FORMAT: UTFDataFormatError,
    UNSUPPORTED_OPERATION: UnsupportedOperationError,
    WRONG_TARGET: WrongTargetError,
    XA: XAError,
    ACCESS_CONTROL: AccessControlError,
    LOGIN: LoginError,
    UNSUPPORTED_CALLBACK: UnsupportedCallbackError,
    NO_DATA_MEMBER: NoDataMemberInClusterError,
    REPLICATED_MAP_CANT_BE_CREATED: ReplicatedMapCantBeCreatedOnLiteMemberError,
    MAX_MESSAGE_SIZE_EXCEEDED: MaxMessageSizeExceededError,
    WAN_REPLICATION_QUEUE_FULL: WANReplicationQueueFullError,
    ASSERTION_ERROR: HazelcastAssertionError,
    OUT_OF_MEMORY_ERROR: OutOfMemoryError,
    STACK_OVERFLOW_ERROR: StackOverflowError,
    NATIVE_OUT_OF_MEMORY_ERROR: NativeOutOfMemoryError,
    SERVICE_NOT_FOUND: ServiceNotFoundError,
    STALE_TASK_ID: StaleTaskIdError,
    DUPLICATE_TASK: DuplicateTaskError,
    STALE_TASK: StaleTaskError,
    LOCAL_MEMBER_RESET: LocalMemberResetError,
    INDETERMINATE_OPERATION_STATE: IndeterminateOperationStateError,
    FLAKE_ID_NODE_ID_OUT_OF_RANGE_EXCEPTION: NodeIdOutOfRangeError,
    TARGET_NOT_REPLICA_EXCEPTION: TargetNotReplicaError,
    MUTATION_DISALLOWED_EXCEPTION: MutationDisallowedError,
    CONSISTENCY_LOST_EXCEPTION: ConsistencyLostError,
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
