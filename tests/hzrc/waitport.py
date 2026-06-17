import asyncio
import socket
import sys


class Protocol(asyncio.BaseProtocol):
    pass


async def connect(host: str, port: int):
    loop = asyncio.get_running_loop()
    while True:
        try:
            return await loop.create_connection(
                lambda: Protocol(),
                host,
                port,
                family=socket.AF_INET,
            )
        except ConnectionRefusedError:
            await asyncio.sleep(0.1)


async def print_waiting_msg(host, port):
    await asyncio.sleep(1)
    print(f"Waiting for {host}:{port} to become available...")


async def amain():
    host = "127.0.0.1"
    port = 9701
    timeout = 120  # seconds
    msg_task = asyncio.create_task(print_waiting_msg(host, port))

    try:
        await asyncio.wait_for(connect(host, port), timeout)
    except TimeoutError:
        print(f"FAILED to connect in {timeout} seconds.")
        sys.exit(1)

    msg_task.cancel()
    print(f"OK, {host}:{port} is up.")
    await asyncio.sleep(1)


if __name__ == "__main__":
    asyncio.run(amain())
