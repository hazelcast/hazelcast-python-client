import hazelcast

client = hazelcast.HazelcastClient()

lock = client.cp_subsystem.get_lock("my-lock").blocking()

locked = lock.is_locked()
print("Locked initially:", locked)

fence = lock.lock_and_get_fence()
print("Fence token:", fence)
try:
    locked = lock.is_locked()
    print("Locked after lock:", locked)

    locked = lock.try_lock()
    print("Locked reentratly:", locked)

    # more guarded code
finally:
    # unlock must be called for each successful lock request
    lock.unlock()
    lock.unlock()

client.shutdown()
