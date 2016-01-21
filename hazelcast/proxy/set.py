from hazelcast.protocol.codec import \
    set_add_all_codec, \
    set_add_codec, \
    set_add_listener_codec, \
    set_clear_codec, \
    set_compare_and_remove_all_codec, \
    set_compare_and_retain_all_codec, \
    set_contains_all_codec, \
    set_contains_codec, \
    set_get_all_codec, \
    set_is_empty_codec, \
    set_remove_codec, \
    set_remove_listener_codec, \
    set_size_codec

from hazelcast.proxy.collection import Collection


class Set(Collection):
    def add(self, item):
        return self._add(item, set_add_codec)

    def add_all(self, items):
        return self._add_all(items, set_add_all_codec)

    def add_listener(self, include_value=False, item_added=None, item_removed=None):
        return self._add_listener(include_value, item_added, item_removed, set_add_listener_codec)

    def clear(self):
        return self._clear(set_clear_codec)

    def contains(self, item):
        return self._contains(item, set_contains_codec)

    def contains_all(self, items):
        return self._contains_all(items, set_contains_all_codec)

    def get_all(self):
        return self._get_all(set_get_all_codec)

    def is_empty(self):
        return self._is_empty(set_is_empty_codec)

    def remove(self, item):
        return self._remove(item, set_remove_codec)

    def remove_all(self, items):
        return self._remove_all(items, set_compare_and_remove_all_codec)

    def remove_listener(self, registration_id):
        return self._remove_listener(registration_id, set_remove_listener_codec)

    def retain_all(self, items):
        return self._retain_all(items, set_compare_and_retain_all_codec)

    def size(self):
        return self._size(set_size_codec)

    def __str__(self):
        return "Set(name=%s)" % self.name
