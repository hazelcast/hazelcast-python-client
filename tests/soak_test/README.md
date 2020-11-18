# Soak Test Description
The test setup at Hazelcast lab environment is:

1. Use 4 member cluster on 2 servers (2 members each).
2. The client test program exercises the following API calls. At most 32 active operations are allowed at any time.
    + Put/Gets
    + Predicates
    + Map Listeners
    + Entry Processors
    
3. Run 10 clients on one lab machine and this machine only runs clients (i.e. no server at this machine)

4. Run the tests for 48 hours. Verify that: 
    + All the client processes are successfully started and shut down.
    + Analyse the outputs: Make sure that there are no errors printed.

# Usage
Run `start_server.sh` on 2 servers 2 times to get 4 members. Make sure to use `hazelcast.xml`.

Run `start_client.sh` on client lab machine 10 times

```bash
./start_client.sh [addresses] [duration]
```

Where 

- **addresses** is the list of cluster member addresses separated by `-`; e.g. `10.212.1.111:5701-10.212.1.111:5702`.
It is enough to give only one member address.
- **duration** is the duration of the SOAK test in hours. Defaults to `48.0`.   