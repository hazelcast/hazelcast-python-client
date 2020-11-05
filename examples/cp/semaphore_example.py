import hazelcast

client = hazelcast.HazelcastClient()

semaphore = client.cp_subsystem.get_semaphore("my-semaphore").blocking()

initialized = semaphore.init(3)
print("Initialized:", initialized)
available = semaphore.available_permits()
print("Available:", available)

semaphore.acquire(3)
available = semaphore.available_permits()
print("Available after acquire:", available)

semaphore.release(2)
available = semaphore.available_permits()
print("Available after release:", available)

client.shutdown()
