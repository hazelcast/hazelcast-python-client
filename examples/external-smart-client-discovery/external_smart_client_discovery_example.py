import logging
import hazelcast

logging.basicConfig(level=logging.INFO)

# Client will try to connect to the cluster using the provided
# public address, and it will connect to the other cluster members
# using their public addresses, if available.
client = hazelcast.HazelcastClient(
    cluster_members=["myserver.publicaddress.com:5701"],
    use_public_ip=True,
)

m = client.get_map("my_map").blocking()

m.set(1, 100)
value = m.get(1)

print(f"Value for the key `1` is {value}.")

client.shutdown()
