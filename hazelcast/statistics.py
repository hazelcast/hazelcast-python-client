import logging
import os

from hazelcast.protocol.codec import client_statistics_codec
from hazelcast.util import calculate_version, current_time_in_millis, to_millis, to_nanos, current_time
from hazelcast.config import ClientProperties
from hazelcast.core import CLIENT_VERSION, CLIENT_TYPE

try:
    import psutil
    PSUTIL_ENABLED = True
except ImportError:
    PSUTIL_ENABLED = False


class Statistics(object):

    _SINCE_VERSION_STRING = "3.9"
    _SINCE_VERSION = calculate_version(_SINCE_VERSION_STRING)

    _NEAR_CACHE_CATEGORY_PREFIX = "nc."
    _STAT_SEPARATOR = ","
    _KEY_VALUE_SEPARATOR = "="
    _EMPTY_STAT_VALUE = ""

    _EXCEPTION_STR = "Could not collect data for the {}. It won't be collected again. {}"

    logger = logging.getLogger("Statistics")

    def __init__(self, client):
        self._client = client
        self._enabled = client.properties.get_bool(ClientProperties.STATISTICS_ENABLED)
        self._cached_owner_address = None
        self._statistics_timer = None
        self._failed_gauges = set()

    def start(self):
        if not self._enabled:
            return

        period = self._client.properties.get_seconds(ClientProperties.STATISTICS_PERIOD_SECONDS)
        if period <= 0:
            default_period = self._client.properties.get_seconds_positive_or_default(
                ClientProperties.STATISTICS_PERIOD_SECONDS)

            self.logger.warning("Provided client statistics {} cannot be less than or equal to 0."
                                "You provided {} as the configuration. Client will use the default value "
                                "{} instead.".format(ClientProperties.STATISTICS_PERIOD_SECONDS.name,
                                                     period,
                                                     default_period))
            period = default_period

        def _statistics_task():
            self._send_statistics()
            self._statistics_timer = self._client.reactor.add_timer(period, _statistics_task)

        self._statistics_timer = self._client.reactor.add_timer(period, _statistics_task)

        self.logger.info("Client statistics enabled with the period of {} seconds.".format(period))

    def shutdown(self):
        if self._statistics_timer:
            self._statistics_timer.cancel()

    def _send_statistics(self):
        owner_connection = self._get_owner_connection()
        if owner_connection is None:
            self.logger.debug("Cannot send client statistics to the server. No owner connection.")
            return

        stats = []
        self._fill_metrics(stats, owner_connection)
        self._add_near_cache_stats(stats)
        self._add_runtime_and_os_stats(stats)
        self._send_stats_to_owner("".join(stats), owner_connection)

    def _send_stats_to_owner(self, stats, owner_connection):
        request = client_statistics_codec.encode_request(stats)
        self._client.invoker.invoke_on_connection(request, owner_connection)

    def _add_runtime_and_os_stats(self, stats):
        os_and_runtime_stats = self._get_os_and_runtime_stats()
        for stat in os_and_runtime_stats:
            self._add_stat(stats, stat, os_and_runtime_stats[stat])

    def _get_os_and_runtime_stats(self):
        psutil_stats = {}
        if PSUTIL_ENABLED:
            try:
                # Availability: All platforms. Try to be sure that it is working.
                if "phy_mem" not in self._failed_gauges:
                    memory_info = psutil.virtual_memory()
                    psutil_stats["os.totalPhysicalMemorySize"] = memory_info.total
                    psutil_stats["os.freePhysicalMemorySize"] = memory_info.available
            except Exception as ex:
                self.logger.warning(self._EXCEPTION_STR.format("physical memory size", ex))
                self._failed_gauges.add("phy_mem")

            try:
                # Availability: All platforms. Try to be sure that it is working.
                if "swap_mem" not in self._failed_gauges:
                    swap_info = psutil.swap_memory()
                    psutil_stats["os.totalSwapSpaceSize"] = swap_info.total
                    psutil_stats["os.freeSwapSpaceSize"] = swap_info.free
            except Exception as ex:
                self.logger.warning(self._EXCEPTION_STR.format("swap space size", ex))
                self._failed_gauges.add("swap_mem")

            try:
                # Availability: Unix. Load average in the last minute
                if "load_avg" not in self._failed_gauges:
                    psutil_stats["os.systemLoadAverage"] = os.getloadavg()[0]
            except Exception as ex:
                self.logger.warning(self._EXCEPTION_STR.format("system load average", ex))
                self._failed_gauges.add("load_avg")

            try:
                # Availability: All platforms. Try to be sure that it is working.
                # Under some platforms, CPU count cannot be determined properly.
                if "cpu_count" not in self._failed_gauges:
                    cpu_count = psutil.cpu_count()
                    if cpu_count:
                        psutil_stats["runtime.availableProcessors"] = cpu_count
                    else:
                        raise ValueError("CPU count cannot be determined.")
            except Exception as ex:
                self.logger.warning(self._EXCEPTION_STR.format("CPU count", ex))
                self._failed_gauges.add("cpu_count")

            process = psutil.Process()
            with process.oneshot():
                # With oneshot, process related information could be gathered
                # faster due to caching.

                # Availability: All platforms. Try to be sure that it is working.
                try:
                    if "p_mem_info" not in self._failed_gauges:
                        p_memory_info = process.memory_info()
                        psutil_stats["os.committedVirtualMemorySize"] = p_memory_info.vms
                        psutil_stats["runtime.usedMemory"] = p_memory_info.rss
                except Exception as ex:
                    self.logger.warning(self._EXCEPTION_STR.format("memory info related to the process", ex))
                    self._failed_gauges.add("p_mem_info")

                try:
                    # Availability: UNIX
                    if "p_fdc" not in self._failed_gauges:
                        psutil_stats["os.openFileDescriptorCount"] = process.num_fds()
                except AttributeError:
                    # Availability: Windows
                    if "p_fdc" not in self._failed_gauges:
                        psutil_stats["os.openFileDescriptorCount"] = process.num_handles()
                except Exception as ex:
                    self.logger.warning(self._EXCEPTION_STR.format("open file descriptor count", ex))
                    self._failed_gauges.add("p_fdc")

                try:
                    # Availability: Linux. Gets the 'hard'/'max' limit
                    if "p_max_fdc" not in self._failed_gauges:
                        psutil_stats["os.maxFileDescriptorCount"] = process.rlimit(psutil.RLIMIT_NOFILE)[1]
                except Exception as ex:
                    self.logger.warning(self._EXCEPTION_STR.format("max file descriptor count", ex))
                    self._failed_gauges.add("p_max_fdc")

                try:
                    # Availability: All platforms. Try to be sure that it is working.
                    if "p_cpu_time" not in self._failed_gauges:
                        psutil_stats["os.processCpuTime"] = to_nanos(sum(process.cpu_times()))
                except Exception as ex:
                    self.logger.warning(self._EXCEPTION_STR.format("process CPU time", ex))
                    self._failed_gauges.add("p_cpu_time")

                try:
                    # Availability: All platforms. Try to be sure that it is working.
                    if "p_uptime" not in self._failed_gauges:
                        psutil_stats["runtime.uptime"] = to_millis(current_time() - process.create_time())
                except Exception as ex:
                    self.logger.warning(self._EXCEPTION_STR.format("process uptime", ex))
                    self._failed_gauges.add("p_uptime")

        return psutil_stats

    def _fill_metrics(self, stats, owner_connection):
        self._add_stat(stats, "lastStatisticsCollectionTime", current_time_in_millis())
        self._add_stat(stats, "enterprise", "false")
        self._add_stat(stats, "clientType", CLIENT_TYPE)
        self._add_stat(stats, "clientVersion", CLIENT_VERSION)
        self._add_stat(stats, "clusterConnectionTimestamp", to_millis(owner_connection.start_time))

        local_host, local_ip = owner_connection.socket.getsockname()
        local_address = str(local_host) + ":" + str(local_ip)
        self._add_stat(stats, "clientAddress", local_address)
        self._add_stat(stats, "clientName", self._client.name)
        self._add_stat(stats, "credentials.principal", self._client.config.group_config.name)

    def _add_near_cache_stats(self, stats):
        for near_cache in self._client.near_cache_manager.list_all_near_caches():
            near_cache_name_with_prefix = self._get_name_with_prefix(near_cache.name)
            near_cache_name_with_prefix.append(".")
            prefix = "".join(near_cache_name_with_prefix)

            near_cache_stats = near_cache.get_statistics()
            self._add_stat(stats, "creationTime", to_millis(near_cache_stats["creation_time"]), prefix)
            self._add_stat(stats, "evictions", near_cache_stats["evictions"], prefix)
            self._add_stat(stats, "hits", near_cache_stats["hits"], prefix)
            self._add_stat(stats, "misses", near_cache_stats["misses"], prefix)
            self._add_stat(stats, "ownedEntryCount", near_cache_stats["entry_count"], prefix)
            self._add_stat(stats, "expirations", near_cache_stats["expirations"], prefix)
            self._add_stat(stats, "ownedEntryMemoryCost", near_cache_stats["size"], prefix)

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

    def _get_owner_connection(self):
        current_owner_address = self._client.cluster.owner_connection_address
        connection = self._client.connection_manager.get_connection(current_owner_address)

        if connection is None:
            return None

        server_version = connection.server_version
        if server_version < Statistics._SINCE_VERSION:
            # do not print too many logs if connected to an old version server
            if self._cached_owner_address and self._cached_owner_address != current_owner_address:
                self.logger.debug("Client statistics cannot be sent to server {} since,"
                                  "connected owner server version is less than the minimum supported server version ,"
                                  "{}.".format(current_owner_address, Statistics._SINCE_VERSION_STRING))

            # cache the last connected server address for decreasing the log prints
            self._cached_owner_address = current_owner_address
            return None

        return connection

    def _get_name_with_prefix(self, name):
        return [Statistics._NEAR_CACHE_CATEGORY_PREFIX, self._escape_special_characters(name)]

    def _escape_special_characters(self, name):
        escaped_name = name.replace("\\", "\\\\").replace(",", "\,").replace(".", "\.").replace("=", "\=")
        return escaped_name[1:] if name[0] == "/" else escaped_name
