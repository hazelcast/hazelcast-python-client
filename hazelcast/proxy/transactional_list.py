import typing

from hazelcast.future import Future
from hazelcast.protocol.codec import (
    transactional_list_add_codec,
    transactional_list_remove_codec,
    transactional_list_size_codec,
)
from hazelcast.proxy.base import TransactionalProxy
from hazelcast.types import ItemType
from hazelcast.util import check_not_none, thread_id


class TransactionalList(TransactionalProxy, typing.Generic[ItemType]):
    """
    Transactional implementation of :class:`~hazelcast.proxy.list.List`.
    """

    def add(self, item: ItemType) -> Future[bool]:
        """Transactional implementation of
        :func:`List.add(item) <hazelcast.proxy.list.List.add>`

        Args:
            item: The new item to be added.

        Returns:
            Future[bool]: ``True`` if the item is added successfully, ``False``
            otherwise.
        """
        check_not_none(item, "item can't be none")
        item_data = self._to_data(item)
        request = transactional_list_add_codec.encode_request(
            self.name, self.transaction.id, thread_id(), item_data
        )
        return self._invoke(request, transactional_list_add_codec.decode_response)

    def remove(self, item: ItemType) -> Future[bool]:
        """Transactional implementation of
        :func:`List.remove(item) <hazelcast.proxy.list.List.remove>`

        Args:
            item: The specified item to be removed.

        Returns:
            Future[bool]: ``True`` if the item is removed successfully,
            ``False`` otherwise.
        """
        check_not_none(item, "item can't be none")
        item_data = self._to_data(item)
        request = transactional_list_remove_codec.encode_request(
            self.name, self.transaction.id, thread_id(), item_data
        )
        return self._invoke(request, transactional_list_remove_codec.decode_response)

    def size(self) -> Future[int]:
        """Transactional implementation of
        :func:`List.size() <hazelcast.proxy.list.List.size>`

        Returns:
            Future[int]: The size of the list.
        """
        request = transactional_list_size_codec.encode_request(
            self.name, self.transaction.id, thread_id()
        )
        return self._invoke(request, transactional_list_size_codec.decode_response)
