from hazelcast.cp import (
    _without_default_group_name,
    _get_object_name_for_proxy,
    ATOMIC_LONG_SERVICE,
    ATOMIC_REFERENCE_SERVICE,
    COUNT_DOWN_LATCH_SERVICE,
)
from hazelcast.internal.asyncio_invocation import Invocation
from hazelcast.internal.asyncio_proxy.atomic_long import AtomicLong
from hazelcast.internal.asyncio_proxy.atomic_reference import AtomicReference
from hazelcast.internal.asyncio_proxy.countdown_latch import CountDownLatch
from hazelcast.protocol.codec import cp_group_create_cp_group_codec


class CPSubsystem:
    """CP Subsystem is a component of Hazelcast that builds a strongly
    consistent layer for a set of distributed data structures.

    Its APIs can be used for implementing distributed coordination use cases,
    such as leader election, distributed locking, synchronization, and metadata
    management.

    Its data structures are CP with respect to the CAP principle, i.e., they
    always maintain linearizability and prefer consistency to availability
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

    async def get_atomic_long(self, name: str) -> AtomicLong:
        """Returns the distributed AtomicLong instance with given name.

        The instance is created on CP Subsystem.

        If no group name is given within the ``name`` argument, then the
        AtomicLong instance will be created on the default CP group.
        If a group name is given, like ``.get_atomic_long("myLong@group1")``,
        the given group will be initialized first, if not initialized
        already, and then the instance will be created on this group.

        Args:
            name: Name of the AtomicLong.

        Returns:
            The AtomicLong proxy for the given name.
        """
        return await self._proxy_manager.get_or_create(ATOMIC_LONG_SERVICE, name)

    async def get_atomic_reference(self, name: str) -> AtomicReference:
        """Returns the distributed AtomicReference instance with given name.

        The instance is created on CP Subsystem.

        If no group name is given within the ``name`` argument, then the
        AtomicReference instance will be created on the DEFAULT CP group.
        If a group name is given, like
        ``.get_atomic_reference("myRef@group1")``, the given group will be
        initialized first, if not initialized already, and then the instance
        will be created on this group.

        Args:
            name: Name of the AtomicReference.

        Returns:
            The AtomicReference proxy for the given name.
        """
        return await self._proxy_manager.get_or_create(ATOMIC_REFERENCE_SERVICE, name)

    async def get_count_down_latch(self, name: str) -> CountDownLatch:
        """Returns the distributed CountDownLatch instance with given name.

        The instance is created on CP Subsystem.

        If no group name is given within the ``name`` argument, then the
        CountDownLatch instance will be created on the DEFAULT CP group.
        If a group name is given, like
        ``.get_count_down_latch("myLatch@group1")``, the given group will be
        initialized first, if not initialized already, and then the instance
        will be created on this group.

        Args:
            name: Name of the CountDownLatch.

        Returns:
            The CountDownLatch proxy for the given name.
        """
        return await self._proxy_manager.get_or_create(COUNT_DOWN_LATCH_SERVICE, name)


class CPProxyManager:
    def __init__(self, context):
        self._context = context

    async def get_or_create(self, service_name, proxy_name):
        proxy_name = _without_default_group_name(proxy_name)
        object_name = _get_object_name_for_proxy(proxy_name)

        group_id = await self._get_group_id(proxy_name)
        if service_name == ATOMIC_LONG_SERVICE:
            return AtomicLong(self._context, group_id, service_name, proxy_name, object_name)
        elif service_name == ATOMIC_REFERENCE_SERVICE:
            return AtomicReference(self._context, group_id, service_name, proxy_name, object_name)
        elif service_name == COUNT_DOWN_LATCH_SERVICE:
            return CountDownLatch(self._context, group_id, service_name, proxy_name, object_name)

        raise ValueError("Unknown service name: %s" % service_name)

    async def _get_group_id(self, proxy_name):
        codec = cp_group_create_cp_group_codec
        request = codec.encode_request(proxy_name)
        invocation = Invocation(request, response_handler=codec.decode_response)
        invocation_service = self._context.invocation_service
        return await invocation_service.ainvoke(invocation)
