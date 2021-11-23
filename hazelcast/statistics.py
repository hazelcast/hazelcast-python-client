import logging
import os

from hazelcast.core import CLIENT_TYPE
from hazelcast.invocation import Invocation
from hazelcast.metrics import MetricsCompressor, MetricDescriptor, ValueType, ProbeUnit
from hazelcast.protocol.codec import client_statistics_codec
from hazelcast.util import current_time_in_millis, to_millis, to_nanos, current_time
from hazelcast import __version__

try:
    import psutil

    _PSUTIL_ENABLED = True
except ImportError:
    _PSUTIL_ENABLED = False

_logger = logging.getLogger(__name__)

_NEAR_CACHE_CATEGORY_PREFIX = "nc."
_ATTRIBUTE_SEPARATOR = ","
_KEY_VALUE_SEPARATOR = "="
_EMPTY_ATTRIBUTE_VALUE = ""

_NEAR_CACHE_DESCRIPTOR_PREFIX = "nearcache"
_NEAR_CACHE_DESCRIPTOR_DISCRIMINATOR = "name"

_TCP_METRICS_PREFIX = "tcp"


class Statistics(object):
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
        self._registered_system_gauges = {}
        self._registered_process_gauges = {}

    def start(self):
        if not self._enabled:
            return

        self._register_gauges()

        def _statistics_task():
            if not self._client.lifecycle_service.is_running():
                return

            try:
                self._collect_and_send_stats()
            finally:
                self._statistics_timer = self._reactor.add_timer(self._period, _statistics_task)

        self._statistics_timer = self._reactor.add_timer(self._period, _statistics_task)

        _logger.info("Client statistics enabled with the period of %s seconds.", self._period)

    def shutdown(self):
        if self._statistics_timer:
            self._statistics_timer.cancel()

    def _register_gauges(self):
        if not _PSUTIL_ENABLED:
            _logger.warning(
                "Statistics collection is enabled, but psutil is not found. "
                "Runtime and system related metrics will not be collected."
            )
            return

        self._register_system_gauge(
            "os.totalPhysicalMemorySize",
            lambda: psutil.virtual_memory().total,
        )
        self._register_system_gauge(
            "os.freePhysicalMemorySize",
            lambda: psutil.virtual_memory().free,
        )
        self._register_system_gauge(
            "os.committedVirtualMemorySize",
            lambda: psutil.virtual_memory().used,
        )
        self._register_system_gauge(
            "os.totalSwapSpaceSize",
            lambda: psutil.swap_memory().total,
        )
        self._register_system_gauge(
            "os.freeSwapSpaceSize",
            lambda: psutil.swap_memory().free,
        )
        self._register_system_gauge(
            "os.systemLoadAverage",
            lambda: os.getloadavg()[0],
            ValueType.DOUBLE,
        )
        self._register_system_gauge(
            "runtime.availableProcessors",
            lambda: psutil.cpu_count(),
        )

        self._register_process_gauge(
            "runtime.usedMemory",
            lambda p: p.memory_info().rss,
        )
        self._register_process_gauge(
            "os.openFileDescriptorCount",
            lambda p: p.num_fds(),
        )
        self._register_process_gauge(
            "os.maxFileDescriptorCount",
            lambda p: p.rlimit(psutil.RLIMIT_NOFILE)[1],
        )
        self._register_process_gauge(
            "os.processCpuTime",
            lambda p: to_nanos(sum(p.cpu_times())),
        )
        self._register_process_gauge(
            "runtime.uptime",
            lambda p: to_millis(current_time() - p.create_time()),
        )

    def _register_system_gauge(self, gauge_name, gauge_fn, value_type=ValueType.LONG):
        # Try a gauge function read, we will register it if it succeeds.
        try:
            gauge_fn()
            self._registered_system_gauges[gauge_name] = (gauge_fn, value_type)
        except Exception as e:
            _logger.debug(
                "Unable to register the system related gauge %s. Error: %s", gauge_name, e
            )

    def _register_process_gauge(self, gauge_name, gauge_fn, value_type=ValueType.LONG):
        # Try a gauge function read, we will register it if it succeeds.
        try:
            process = psutil.Process()
            gauge_fn(process)
            self._registered_process_gauges[gauge_name] = (gauge_fn, value_type)
        except Exception as e:
            _logger.debug(
                "Unable to register the process related gauge %s. Error: %s", gauge_name, e
            )

    def _collect_and_send_stats(self):
        connection = self._connection_manager.get_random_connection()
        if not connection:
            _logger.debug("Cannot send client statistics to the server. No connection found.")
            return

        collection_timestamp = current_time_in_millis()
        attributes = []
        compressor = MetricsCompressor()

        self._add_client_attributes(attributes, connection)
        self._add_near_cache_metrics(attributes, compressor)
        self._add_system_and_process_metrics(attributes, compressor)
        self._add_tcp_metrics(compressor)
        self._send_stats(
            collection_timestamp, "".join(attributes), compressor.generate_blob(), connection
        )

    def _send_stats(self, collection_timestamp, attributes, metrics_blob, connection):
        request = client_statistics_codec.encode_request(
            collection_timestamp, attributes, metrics_blob
        )
        invocation = Invocation(request, connection=connection)
        self._invocation_service.invoke(invocation)

    def _add_system_and_process_metrics(self, attributes, compressor):
        if not _PSUTIL_ENABLED:
            # Nothing to do if psutil is not found
            return

        for gauge_name, (gauge_fn, value_type) in self._registered_system_gauges.items():
            try:
                value = gauge_fn()
                self._add_system_or_process_metric(
                    attributes, compressor, gauge_name, value, value_type
                )
            except:
                _logger.exception("Error while collecting '%s'.", gauge_name)

        if not self._registered_process_gauges:
            # Do not create the process object if no process-related
            # metric is registered.
            return

        process = psutil.Process()
        for gauge_name, (gauge_fn, value_type) in self._registered_process_gauges.items():
            try:
                value = gauge_fn(process)
                self._add_system_or_process_metric(
                    attributes, compressor, gauge_name, value, value_type
                )
            except:
                _logger.exception("Error while collecting '%s'.", gauge_name)

    def _add_system_or_process_metric(self, attributes, compressor, gauge_name, value, value_type):
        # We don't have any metrics that do not have prefix.
        # Necessary care must be taken when we will send simple
        # named metrics.
        prefix, metric_name = gauge_name.rsplit(".", 1)
        descriptor = MetricDescriptor(metric=metric_name, prefix=prefix)
        self._add_metric(compressor, descriptor, value, value_type)
        self._add_attribute(attributes, gauge_name, value)

    def _add_client_attributes(self, attributes, connection):
        self._add_attribute(attributes, "lastStatisticsCollectionTime", current_time_in_millis())
        self._add_attribute(attributes, "enterprise", "false")
        self._add_attribute(attributes, "clientType", CLIENT_TYPE)
        self._add_attribute(attributes, "clientVersion", __version__)
        self._add_attribute(
            attributes, "clusterConnectionTimestamp", to_millis(connection.start_time)
        )

        local_address = connection.local_address
        local_address = str(local_address.host) + ":" + str(local_address.port)
        self._add_attribute(attributes, "clientAddress", local_address)
        self._add_attribute(attributes, "clientName", self._client.name)

    def _add_near_cache_metrics(self, attributes, compressor):
        for near_cache in self._near_cache_manager.list_near_caches():
            nc_name = near_cache.name
            nc_name_with_prefix = self._get_name_with_prefix(nc_name)
            nc_name_with_prefix.append(".")
            nc_name_with_prefix = "".join(nc_name_with_prefix)

            near_cache_stats = near_cache.get_statistics()
            self._add_near_cache_metric(
                attributes,
                compressor,
                "creationTime",
                to_millis(near_cache_stats["creation_time"]),
                ValueType.LONG,
                ProbeUnit.MS,
                nc_name,
                nc_name_with_prefix,
            )

            self._add_near_cache_metric(
                attributes,
                compressor,
                "evictions",
                near_cache_stats["evictions"],
                ValueType.LONG,
                ProbeUnit.COUNT,
                nc_name,
                nc_name_with_prefix,
            )

            self._add_near_cache_metric(
                attributes,
                compressor,
                "hits",
                near_cache_stats["hits"],
                ValueType.LONG,
                ProbeUnit.COUNT,
                nc_name,
                nc_name_with_prefix,
            )

            self._add_near_cache_metric(
                attributes,
                compressor,
                "misses",
                near_cache_stats["misses"],
                ValueType.LONG,
                ProbeUnit.COUNT,
                nc_name,
                nc_name_with_prefix,
            )

            self._add_near_cache_metric(
                attributes,
                compressor,
                "ownedEntryCount",
                near_cache_stats["owned_entry_count"],
                ValueType.LONG,
                ProbeUnit.COUNT,
                nc_name,
                nc_name_with_prefix,
            )

            self._add_near_cache_metric(
                attributes,
                compressor,
                "expirations",
                near_cache_stats["expirations"],
                ValueType.LONG,
                ProbeUnit.COUNT,
                nc_name,
                nc_name_with_prefix,
            )

            self._add_near_cache_metric(
                attributes,
                compressor,
                "invalidations",
                near_cache_stats["invalidations"],
                ValueType.LONG,
                ProbeUnit.COUNT,
                nc_name,
                nc_name_with_prefix,
            )

            self._add_near_cache_metric(
                attributes,
                compressor,
                "invalidationRequests",
                near_cache_stats["invalidation_requests"],
                ValueType.LONG,
                ProbeUnit.COUNT,
                nc_name,
                nc_name_with_prefix,
            )

            self._add_near_cache_metric(
                attributes,
                compressor,
                "ownedEntryMemoryCost",
                near_cache_stats["owned_entry_memory_cost"],
                ValueType.LONG,
                ProbeUnit.BYTES,
                nc_name,
                nc_name_with_prefix,
            )

    def _add_near_cache_metric(
        self, attributes, compressor, metric, value, value_type, unit, nc_name, nc_name_with_prefix
    ):
        descriptor = MetricDescriptor(
            metric=metric,
            prefix=_NEAR_CACHE_DESCRIPTOR_PREFIX,
            discriminator=_NEAR_CACHE_DESCRIPTOR_DISCRIMINATOR,
            discriminator_value=nc_name,
            unit=unit,
        )
        try:
            self._add_metric(compressor, descriptor, value, value_type)
            self._add_attribute(attributes, metric, value, nc_name_with_prefix)
        except:
            _logger.exception(
                "Error while collecting %s metric for near cache '%s'.", metric, nc_name
            )

    def _add_tcp_metrics(self, compressor):
        self._add_tcp_metric(compressor, "bytesSend", self._reactor.bytes_sent)
        self._add_tcp_metric(compressor, "bytesReceived", self._reactor.bytes_received)

    def _add_tcp_metric(
        self, compressor, metric, value, value_type=ValueType.LONG, unit=ProbeUnit.BYTES
    ):
        descriptor = MetricDescriptor(
            metric=metric,
            prefix=_TCP_METRICS_PREFIX,
            unit=unit,
        )
        try:
            self._add_metric(compressor, descriptor, value, value_type)
        except:
            _logger.exception("Error while collecting '%s.%s'.", _TCP_METRICS_PREFIX, metric)

    def _add_metric(self, compressor, descriptor, value, value_type):
        if value_type == ValueType.LONG:
            compressor.add_long(descriptor, value)
        elif value_type == ValueType.DOUBLE:
            compressor.add_double(descriptor, value)
        else:
            raise ValueError("Unexpected type: " + value_type)

    def _add_attribute(self, attributes, name, value, key_prefix=None):
        if len(attributes) != 0:
            attributes.append(_ATTRIBUTE_SEPARATOR)

        if key_prefix:
            attributes.append(key_prefix)

        attributes.append(name)
        attributes.append(_KEY_VALUE_SEPARATOR)
        attributes.append(str(value))

    def _get_name_with_prefix(self, name):
        return [_NEAR_CACHE_CATEGORY_PREFIX, self._escape_special_characters(name)]

    def _escape_special_characters(self, name):
        escaped_name = (
            name.replace("\\", "\\\\").replace(",", "\\,").replace(".", "\\.").replace("=", "\\=")
        )
        return escaped_name[1:] if name[0] == "/" else escaped_name
