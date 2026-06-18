from contextlib import AbstractAsyncContextManager


class LockContext(AbstractAsyncContextManager):
    def __init__(self, proxy, key):
        self._proxy = proxy
        self._key = key
        self._token = None

    async def __aenter__(self):
        await self._proxy.lock(self._key)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._proxy.unlock(self._key)
