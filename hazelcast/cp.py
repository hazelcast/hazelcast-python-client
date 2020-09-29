from hazelcast.invocation import Invocation
from hazelcast.protocol.codec import cp_group_create_cp_group_codec
from hazelcast.proxy.cp.atomic_long import AtomicLong
from hazelcast.util import check_true


class CPSubsystem(object):
    """CP Subsystem is a component of Hazelcast that builds a strongly consistent
    layer for a set of distributed data structures.

    Its APIs can be used for implementing distributed coordination use cases,
    such as leader election, distributed locking, synchronization, and metadata
    management.

    Its data structures are CP with respect to the CAP principle, i.e., they
    always maintain linearizability and prefer consistency over availability
    during network partitions. Besides network partitions, CP Subsystem
    withstands server and client failures.

    Data structures in CP Subsystem run in CP groups. Each CP group elects
    its own Raft leader and runs the Raft consensus algorithm independently.

    The CP data structures differ from the other Hazelcast data structures
    in two aspects. First, an internal commit is performed on the METADATA CP
    group every time you fetch a proxy from this interface. Hence, callers
    should cache returned proxy objects. Second, if you call ``destroy()``
    on a CP data structure proxy, that data structure is terminated on the
    underlying CP group and cannot be reinitialized until the CP group is
    force-destroyed. For this reason, please make sure that you are completely
    done with a CP data structure before destroying its proxy.
    """

    def __init__(self, context):
        self._proxy_manager = CPProxyManager(context)

    def get_atomic_long(self, name):
        """Returns the distributed AtomicLong instance with given name.

        The instance is created on CP Subsystem.

        If no group name is given within the ``name`` argument, then the
        AtomicLong instance will be created on the default CP group.
        If a group name is given, like ``.get_atomic_long("myLong@group1")``,
        the given group will be initialized first, if not initialized
        already, and then the instance will be created on this group.

        Args:
            name (str): Name of the AtomicLong.

        Returns:
            hazelcast.proxy.cp.atomic_long.AtomicLong: The AtomicLong proxy
                for the given name.
        """
        return self._proxy_manager.get_or_create(ATOMIC_LONG_SERVICE, name)


_DEFAULT_GROUP_NAME = "default"


def _with_default_group_name(name):
    name = name.strip()
    idx = name.find("@")
    if idx == -1:
        return name

    check_true(name.find("@", idx + 1) == -1, "Custom group name must be specified at most once")
    group_name = name[idx + 1:].strip()
    if group_name == _DEFAULT_GROUP_NAME:
        return name[:idx]
    return name


def _get_object_name_for_proxy(name):
    idx = name.find("@")
    if idx == -1:
        return name

    group_name = name[idx + 1:].strip()
    check_true(len(group_name) > 0, "Custom CP group name cannot be empty string")
    object_name = name[:idx].strip()
    check_true(len(object_name) > 0, "Object name cannot be empty string")
    return object_name


ATOMIC_LONG_SERVICE = "hz:raft:atomicLongService"


class CPProxyManager(object):
    def __init__(self, context):
        self._context = context

    def get_or_create(self, service_name, proxy_name):
        proxy_name = _with_default_group_name(proxy_name)
        object_name = _get_object_name_for_proxy(proxy_name)

        group_id = self._get_group_id(proxy_name)
        if service_name == ATOMIC_LONG_SERVICE:
            return AtomicLong(self._context, group_id, service_name, proxy_name, object_name)

    def _get_group_id(self, proxy_name):
        codec = cp_group_create_cp_group_codec
        request = codec.encode_request(proxy_name)
        invocation = Invocation(request, response_handler=codec.decode_response)
        invocation_service = self._context.invocation_service
        invocation_service.invoke(invocation)
        return invocation.future.result()
