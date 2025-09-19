import asyncio


def future_continue_with(future: asyncio.Future, callback) -> asyncio.Future:
    future.add_done_callback(callback)
