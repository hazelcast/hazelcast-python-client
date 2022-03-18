import logging
import hazelcast

logging.basicConfig(level=logging.INFO)

client = hazelcast.HazelcastClient()

lock = client.cp_subsystem.get_lock("my-lock").blocking()

locked = lock.is_locked()
print(f"Locked initially: {locked}")

fence = lock.lock()
print(f"Fence token: {fence}")
try:
    locked = lock.is_locked()
    print(f"Locked after lock: {locked}")

    fence = lock.try_lock()
    print(f"Locked reentrantly: {fence != lock.INVALID_FENCE}")

    # more guarded code
finally:
    # unlock must be called for each successful lock request
    lock.unlock()
    lock.unlock()

client.shutdown()
