import logging
import hazelcast

logging.basicConfig(level=logging.INFO)

client = hazelcast.HazelcastClient()

semaphore = client.cp_subsystem.get_semaphore("my-semaphore").blocking()

initial_permits = semaphore.init(3)
print(f"Initialized with: {initial_permits}")

available = semaphore.available_permits()
print(f"Available permits: {available}")

semaphore.acquire(3)
available = semaphore.available_permits()
print(f"Available permits after acquire: {available}")

semaphore.release(2)
available = semaphore.available_permits()
print(f"Available permits after release: {available}")

client.shutdown()
