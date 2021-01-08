import logging
import os

from hazelcast.core import CLIENT_TYPE
from hazelcast.invocation import Invocation
from hazelcast.protocol.codec import client_statistics_codec
from hazelcast.util import current_time_in_millis, to_millis, to_nanos, current_time
from hazelcast import six, __version__

try:
    import psutil

    PSUTIL_ENABLED = True
except ImportError:
    PSUTIL_ENABLED = False

_logger = logging.getLogger(__name__)


class Statistics(object):
    _NEAR_CACHE_CATEGORY_PREFIX = "nc."
    _STAT_SEPARATOR = ","
    _KEY_VALUE_SEPARATOR = "="
    _EMPTY_STAT_VALUE = ""

    _DEFAULT_PROBE_VALUE = 0

    def __init__(
        self, client, config, reactor, connection_manager, invocation_service, near_cache_manager
    ):
        self._client = client
        self._reactor = reactor
        self._connection_manager = connection_manager
        self._invocation_service = invocation_service
        self._near_cache_manager = near_cache_manager
        self._enabled = config.statistics_enabled
        self._period = config.statistics_period
        self._statistics_timer = None
        self._failed_gauges = set()

    def start(self):
        if not self._enabled:
            return

        def _statistics_task():
            if not self._client.lifecycle_service.is_running():
                return

            self._send_statistics()
            self._statistics_timer = self._reactor.add_timer(self._period, _statistics_task)

        self._statistics_timer = self._reactor.add_timer(self._period, _statistics_task)

        _logger.info("Client statistics enabled with the period of %s seconds.", self._period)

    def shutdown(self):
        if self._statistics_timer:
            self._statistics_timer.cancel()

    def _send_statistics(self):
        connection = self._connection_manager.get_random_connection()
        if not connection:
            _logger.debug("Cannot send client statistics to the server. No connection found.")
            return

        collection_timestamp = current_time_in_millis()
        stats = []
        self._fill_metrics(stats, connection)
        self._add_near_cache_stats(stats)
        self._add_runtime_and_os_stats(stats)
        self._send_stats_to_owner(collection_timestamp, "".join(stats), connection)

    def _send_stats_to_owner(self, collection_timestamp, stats, connection):
        request = client_statistics_codec.encode_request(collection_timestamp, stats, bytearray(0))
        invocation = Invocation(request, connection=connection)
        self._invocation_service.invoke(invocation)

    def _add_runtime_and_os_stats(self, stats):
        os_and_runtime_stats = self._get_os_and_runtime_stats()
        for stat_name, stat_value in six.iteritems(os_and_runtime_stats):
            self._add_stat(stats, stat_name, stat_value)

    def _get_os_and_runtime_stats(self):
        psutil_stats = {}
        if PSUTIL_ENABLED:

            if self._can_collect_stat("os.totalPhysicalMemorySize") or self._can_collect_stat(
                "os.freePhysicalMemorySize"
            ):
                self._collect_physical_memory_info(
                    psutil_stats, "os.totalPhysicalMemorySize", "os.freePhysicalMemorySize"
                )

            if self._can_collect_stat("os.totalSwapSpaceSize") or self._can_collect_stat(
                "os.freeSwapSpaceSize"
            ):
                self._collect_swap_memory_info(
                    psutil_stats, "os.totalSwapSpaceSize", "os.freeSwapSpaceSize"
                )

            if self._can_collect_stat("os.systemLoadAverage"):
                self._collect_load_average(psutil_stats, "os.systemLoadAverage")

            if self._can_collect_stat("runtime.availableProcessors"):
                self._collect_cpu_count(psutil_stats, "runtime.availableProcessors")

            process = psutil.Process()
            with process.oneshot():
                # With oneshot, process related information could be gathered
                # faster due to caching.

                if self._can_collect_stat(
                    "os.committedVirtualMemorySize"
                ) or self._can_collect_stat("runtime.usedMemory"):
                    self._collect_process_memory_info(
                        psutil_stats, "os.committedVirtualMemorySize", "runtime.usedMemory", process
                    )

                if self._can_collect_stat("os.openFileDescriptorCount"):
                    self._collect_file_descriptor_count(
                        psutil_stats, "os.openFileDescriptorCount", process
                    )

                if self._can_collect_stat("os.maxFileDescriptorCount"):
                    self._collect_max_file_descriptor_count(
                        psutil_stats, "os.maxFileDescriptorCount", process
                    )

                if self._can_collect_stat("os.processCpuTime"):
                    self._collect_process_cpu_time(psutil_stats, "os.processCpuTime", process)

                if self._can_collect_stat("runtime.uptime"):
                    self._collect_process_uptime(psutil_stats, "runtime.uptime", process)

        return psutil_stats

    def _fill_metrics(self, stats, connection):
        self._add_stat(stats, "lastStatisticsCollectionTime", current_time_in_millis())
        self._add_stat(stats, "enterprise", "false")
        self._add_stat(stats, "clientType", CLIENT_TYPE)
        self._add_stat(stats, "clientVersion", __version__)
        self._add_stat(stats, "clusterConnectionTimestamp", to_millis(connection.start_time))

        local_address = connection.local_address
        local_address = str(local_address.host) + ":" + str(local_address.port)
        self._add_stat(stats, "clientAddress", local_address)
        self._add_stat(stats, "clientName", self._client.name)

    def _add_near_cache_stats(self, stats):
        for near_cache in self._near_cache_manager.list_near_caches():
            near_cache_name_with_prefix = self._get_name_with_prefix(near_cache.name)
            near_cache_name_with_prefix.append(".")
            prefix = "".join(near_cache_name_with_prefix)

            near_cache_stats = near_cache.get_statistics()
            self._add_stat(
                stats, "creationTime", to_millis(near_cache_stats["creation_time"]), prefix
            )
            self._add_stat(stats, "evictions", near_cache_stats["evictions"], prefix)
            self._add_stat(stats, "hits", near_cache_stats["hits"], prefix)
            self._add_stat(stats, "misses", near_cache_stats["misses"], prefix)
            self._add_stat(stats, "ownedEntryCount", near_cache_stats["owned_entry_count"], prefix)
            self._add_stat(stats, "expirations", near_cache_stats["expirations"], prefix)
            self._add_stat(stats, "invalidations", near_cache_stats["invalidations"], prefix)
            self._add_stat(
                stats, "invalidationRequests", near_cache_stats["invalidation_requests"], prefix
            )
            self._add_stat(
                stats, "ownedEntryMemoryCost", near_cache_stats["owned_entry_memory_cost"], prefix
            )

    def _add_stat(self, stats, name, value, key_prefix=None):
        if len(stats) != 0:
            stats.append(Statistics._STAT_SEPARATOR)

        if key_prefix:
            stats.append(key_prefix)

        stats.append(name)
        stats.append(Statistics._KEY_VALUE_SEPARATOR)
        stats.append(str(value))

    def _add_empty_stat(self, stats, name, key_prefix=None):
        self._add_stat(stats, name, Statistics._EMPTY_STAT_VALUE, key_prefix)

    def _get_name_with_prefix(self, name):
        return [Statistics._NEAR_CACHE_CATEGORY_PREFIX, self._escape_special_characters(name)]

    def _escape_special_characters(self, name):
        escaped_name = (
            name.replace("\\", "\\\\").replace(",", "\\,").replace(".", "\\.").replace("=", "\\=")
        )
        return escaped_name[1:] if name[0] == "/" else escaped_name

    def _can_collect_stat(self, name):
        return name not in self._failed_gauges

    def _safe_psutil_stat_collector(func):
        def safe_wrapper(self, psutil_stats, probe_name, *args):
            # Decorated function's signature must match with above
            try:
                stat = func(self, psutil_stats, probe_name, *args)
            except AttributeError as ae:
                _logger.debug(
                    "Unable to register psutil method used for the probe %s. " "Cause: %s",
                    probe_name,
                    ae,
                )
                self._failed_gauges.add(probe_name)
                return
            except Exception as ex:
                _logger.warning("Failed to access the probe %s. Cause: %s", probe_name, ex)
                stat = self._DEFAULT_PROBE_VALUE

            psutil_stats[probe_name] = stat

        return safe_wrapper

    def _collect_physical_memory_info(self, psutil_stats, probe_total, probe_free):
        memory_info = psutil.virtual_memory()

        if self._can_collect_stat(probe_total):
            self._collect_total_physical_memory_size(psutil_stats, probe_total, memory_info)

        if self._can_collect_stat(probe_free):
            self._collect_free_physical_memory_size(psutil_stats, probe_free, memory_info)

    @_safe_psutil_stat_collector
    def _collect_total_physical_memory_size(self, psutil_stats, probe_name, memory_info):
        return memory_info.total

    @_safe_psutil_stat_collector
    def _collect_free_physical_memory_size(self, psutil_stats, probe_name, memory_info):
        return memory_info.available

    def _collect_swap_memory_info(self, psutil_stats, probe_total, probe_free):
        swap_info = psutil.swap_memory()

        if self._can_collect_stat(probe_total):
            self._collect_total_swap_space(psutil_stats, probe_total, swap_info)

        if self._can_collect_stat(probe_free):
            self._collect_free_swap_space(psutil_stats, probe_free, swap_info)

    @_safe_psutil_stat_collector
    def _collect_total_swap_space(self, psutil_stats, probe_name, swap_info):
        return swap_info.total

    @_safe_psutil_stat_collector
    def _collect_free_swap_space(self, psutil_stats, probe_name, swap_info):
        return swap_info.free

    @_safe_psutil_stat_collector
    def _collect_load_average(self, psutil_stats, probe_name):
        return os.getloadavg()[0]

    @_safe_psutil_stat_collector
    def _collect_cpu_count(self, psutil_stats, probe_name):
        return psutil.cpu_count()

    def _collect_process_memory_info(self, psutil_stats, probe_cvms, probe_used, process):
        p_memory_info = process.memory_info()

        if self._can_collect_stat(probe_cvms):
            self._collect_committed_virtual_memory_size(psutil_stats, probe_cvms, p_memory_info)

        if self._can_collect_stat(probe_used):
            self._collect_used_memory(psutil_stats, probe_used, p_memory_info)

    @_safe_psutil_stat_collector
    def _collect_committed_virtual_memory_size(self, psutil_stats, probe_name, p_memory_info):
        return p_memory_info.vms

    @_safe_psutil_stat_collector
    def _collect_used_memory(self, psutil_stats, probe_name, p_memory_info):
        return p_memory_info.rss

    @_safe_psutil_stat_collector
    def _collect_file_descriptor_count(self, psutil_stats, probe_name, process):
        try:
            return process.num_fds()
        except AttributeError:
            # Fallback for Windows
            return process.num_handles()

    @_safe_psutil_stat_collector
    def _collect_max_file_descriptor_count(self, psutil_stats, probe_name, process):
        return process.rlimit(psutil.RLIMIT_NOFILE)[1]

    @_safe_psutil_stat_collector
    def _collect_process_cpu_time(self, psutil_stats, probe_name, process):
        return to_nanos(sum(process.cpu_times()))

    @_safe_psutil_stat_collector
    def _collect_process_uptime(self, psutil_stats, probe_name, process):
        return to_millis(current_time() - process.create_time())
