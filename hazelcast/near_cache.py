import logging
import random

from hazelcast.config import EVICTION_POLICY, IN_MEMORY_FORMAT
from hazelcast.util import current_time
from hazelcast.six.moves import range
from sys import getsizeof


def lru_key_func(x):
    """
    Least Recently Used key function.

    :param x: (:class:`~hazelcast.near_cache.DataRecord`)
    :return: (float), last access time of x.
    """
    return x.last_access_time


def lfu_key_func(x):
    """
    Least Frequently Used key function.

    :param x: (:class:`~hazelcast.near_cache.DataRecord`)
    :return: (int), access hit count of x.
    """
    return x.access_hit


def random_key_func(x):
    """
    Random key function.

    :param x: (:class:`~hazelcast.near_cache.DataRecord`)
    :return: (int), 0.
    """
    return 0


eviction_key_func = {EVICTION_POLICY.NONE: None, EVICTION_POLICY.LRU: lru_key_func, EVICTION_POLICY.LFU: lfu_key_func,
                     EVICTION_POLICY.RANDOM: random_key_func}


class DataRecord(object):
    """
    An expirable and evictable data object which represents a cache entry.
    """
    def __init__(self, key, value, create_time=None, ttl_seconds=None):
        self.key = key
        self.value = value
        self.create_time = create_time if create_time is not None else current_time()
        self.expiration_time = self.create_time + ttl_seconds if ttl_seconds is not None else None
        self.last_access_time = self.create_time
        self.access_hit = 0

    def is_expired(self, max_idle_seconds):
        """
        Determines whether this record is expired or not.

        :param max_idle_seconds: (long), the maximum idle time of record, maximum time after the last access time.
        :return: (bool), ``true`` is this record is not expired.
        """

        now = current_time()
        return (self.expiration_time is not None and self.expiration_time < now) or \
               (max_idle_seconds is not None and self.last_access_time + max_idle_seconds < now)

    def __repr__(self):
        return "DataRecord[key:{}, value:{}, create_time:{}, expiration_time:{}, last_access_time={}, access_hit={}]" \
            .format(self.key, self.value, self.create_time, self.expiration_time, self.last_access_time, self.access_hit)


class NearCache(dict):
    """
    NearCache is a local cache used by :class:`~hazelcast.proxy.map.MapFeatNearCache`.
    """

    def __init__(self, name, serialization_service, in_memory_format, time_to_live_seconds, max_idle_seconds, invalidate_on_change,
                 eviction_policy, eviction_max_size, eviction_sampling_count=None, eviction_sampling_pool_size=None):
        self.name = name
        self.serialization_service = serialization_service
        self.in_memory_format = in_memory_format
        self.time_to_live_seconds = time_to_live_seconds
        self.max_idle_seconds = max_idle_seconds
        self.invalidate_on_change = invalidate_on_change
        self.eviction_policy = eviction_policy
        self.eviction_max_size = eviction_max_size

        if eviction_sampling_count is None:  # None or zero
            self.eviction_sampling_count = max(eviction_max_size // 10, 1)
        elif 0 < eviction_sampling_count <= self.eviction_max_size:
            self.eviction_sampling_count = eviction_sampling_count
        else:
            self.eviction_sampling_count = self.eviction_max_size

        if eviction_sampling_pool_size is None:  # None or zero
            self.eviction_sampling_pool_size = max(eviction_max_size // 5, 1)
        elif 0 < eviction_sampling_pool_size <= self.eviction_max_size:
            self.eviction_sampling_pool_size = eviction_sampling_pool_size
        else:
            self.eviction_sampling_pool_size = self.eviction_max_size

        # internal
        self._key_func = eviction_key_func[self.eviction_policy]
        self._eviction_candidates = list()
        self._evictions = 0
        self._expirations = 0
        self._hits = 0
        self._misses = 0
        self._invalidations = 0
        self._invalidation_requests = 0
        self._creation_time_in_seconds = current_time()

    def get_statistics(self):
        """
        Returns the statistics of the NearCache.
        :return: (Dict), Dictionary that stores statistics related to this near cache.
        """
        stats = {
            "creation_time": self._creation_time_in_seconds,
            "evictions": self._evictions,
            "expirations": self._expirations,
            "misses": self._misses,
            "hits": self._hits,
            "invalidations": self._invalidations,
            "invalidation_requests": self._invalidation_requests,
            "owned_entry_count": self.__len__(),
            "owned_entry_memory_cost": getsizeof(self),
        }

        return stats

    def __setitem__(self, key, value):
        self._do_eviction_if_required()

        if self.in_memory_format == IN_MEMORY_FORMAT.BINARY:
            value = self.serialization_service.to_data(value)
        elif self.in_memory_format == IN_MEMORY_FORMAT.OBJECT:
            value = self.serialization_service.to_object(value)
        else:
            raise ValueError("Invalid in-memory format!!!")

        data_record = DataRecord(key, value, ttl_seconds=self.time_to_live_seconds)
        super(NearCache, self).__setitem__(key, data_record)

    def __getitem__(self, key):
        try:
            value_record = super(NearCache, self).__getitem__(key)
            if value_record.is_expired(self.max_idle_seconds):
                super(NearCache, self).__delitem__(key)
                raise KeyError
        except KeyError as ke:
            self._misses += 1
            raise ke

        if self.eviction_policy == EVICTION_POLICY.LRU:
            value_record.last_access_time = current_time()
        elif self.eviction_policy == EVICTION_POLICY.LFU:
            value_record.access_hit += 1
        self._hits += 1
        return self.serialization_service.to_object(value_record.value) \
            if self.in_memory_format == IN_MEMORY_FORMAT.BINARY else value_record.value

    def _do_eviction_if_required(self):
        if not self._is_eviction_required():
            return
        new_eviction_samples = self._find_new_random_samples()
        new_eviction_samples_cleaned = self._scan_and_expire_collection(new_eviction_samples)
        if len(new_eviction_samples_cleaned) == 0:  # have nothing to expire
            return

        sorted_candidate_pool = sorted(new_eviction_samples_cleaned, key=self._key_func)
        min_size = min(self.eviction_sampling_pool_size, len(sorted_candidate_pool))
        self._eviction_candidates = sorted_candidate_pool[:min_size]  # set new eviction candidate pool

        if len(new_eviction_samples) == len(new_eviction_samples_cleaned):  # did any item expired or do we need to evict
            try:
                self.__delitem__(self._eviction_candidates[0].key)
                self._evictions += 1
                del self._eviction_candidates[0]
            except KeyError:
                # key may be evicted previously so just ignore it
                pass

    def _find_new_random_samples(self):
        records = list(self.values())  # has random order because of dict hash
        new_sample_pool = set(self._eviction_candidates)
        start = self._random_index()
        for i in range(start, start + self.eviction_sampling_count):
            index = i if i < len(records) else i - len(records)
            if records[index].is_expired(self.max_idle_seconds):
                self._clean_expired_record(records[index].key)
            elif self._is_better_than_worse_entry(records[index]) or len(new_sample_pool) < self.eviction_sampling_pool_size:
                new_sample_pool.add(records[index])
        return new_sample_pool

    def _scan_and_expire_collection(self, records):
        new_records = []
        for record in records:
            if record.is_expired(self.max_idle_seconds):
                self._clean_expired_record(record.key)
            else:
                new_records.append(record)
        return new_records

    def _random_index(self):
        return random.randint(0, self.eviction_max_size - 1)

    def _is_better_than_worse_entry(self, data_record):
        return len(self._eviction_candidates) == 0 \
               or (self._key_func(data_record) - self._key_func(self._eviction_candidates[-1])) < 0

    def _is_eviction_required(self):
        return self.eviction_policy != EVICTION_POLICY.NONE and self.eviction_max_size <= self.__len__()

    def _clean_expired_record(self, key):
        try:
            self.__delitem__(key)
            self._expirations += 1
        except KeyError:
            # key may be evicted previously so just ignore it
            pass

    def _clear(self):
        size = self.__len__()
        self.clear()
        self._invalidations += size
        self._invalidation_requests += 1

    def _invalidate(self, key_data):
        try:
            self.__delitem__(key_data)
            self._invalidations += 1
        except KeyError:
            # There is nothing to invalidate
            pass
        self._invalidation_requests += 1

    def __repr__(self):
        return "NearCache[len:{}, evicted:{}]".format(self.__len__(), self._evictions)


class NearCacheManager(object):
    def __init__(self, client):
        self._client = client
        self._caches = {}

    def get_or_create_near_cache(self, name):
        near_cache = self._caches.get(name, None)
        if not near_cache:
            near_cache_config = self._client.config.near_cache_configs.get(name, None)
            if not near_cache_config:
                raise ValueError("Cannot find a near cache configuration with the name '{}'".format(name))

            near_cache = NearCache(near_cache_config.name,
                                   self._client.serialization_service,
                                   near_cache_config.in_memory_format,
                                   near_cache_config.time_to_live_seconds,
                                   near_cache_config.max_idle_seconds,
                                   near_cache_config.invalidate_on_change,
                                   near_cache_config.eviction_policy,
                                   near_cache_config.eviction_max_size,
                                   near_cache_config.eviction_sampling_count,
                                   near_cache_config.eviction_sampling_pool_size)

            self._caches[name] = near_cache

        return near_cache

    def destroy_near_cache(self, name):
        try:
            near_cache = self._caches.pop(name)
            near_cache.clear()
        except KeyError:
            pass

    def destroy_all_near_caches(self):
        for key in list(self._caches.keys()):
            self.destroy_near_cache(key)

    def list_all_near_caches(self):
        return list(self._caches.values())
