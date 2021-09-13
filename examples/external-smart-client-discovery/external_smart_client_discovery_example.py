import hazelcast

# Client will try to connect to the cluster using the provided
# public address and it will connect to the other cluster members
# using their public addresses, if available.
client = hazelcast.HazelcastClient(
    cluster_members=["myserver.publicaddress.com:5701"],
    use_public_addresses=True,
)

m = client.get_map("my-map").blocking()

m.set(1, 100)
value = m.get(1)

print("Value for the key `1` is %s." % value)

client.shutdown()
