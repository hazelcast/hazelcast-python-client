import hazelcast

client = hazelcast.HazelcastClient()

pn_counter = client.get_pn_counter("pn-counter").blocking()

print("Counter is initialized with", pn_counter.get())

for i in range(10):
    print("Added %s to the counter. Current value is %s" % (i, pn_counter.add_and_get(i)))

print("Incremented the counter after getting the current value. "
      "Previous value is", pn_counter.get_and_increment())

print("Final value is", pn_counter.get())
