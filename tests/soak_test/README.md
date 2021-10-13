# SOAK Test Description

SOAK test for the Hazelcast Python client should be executed in an 
environment described below.

- There should be 3 machines involved. Let's call those machines
`A`, `B`, and `C`.
- Two of those will be used for members, and the remaining one 
will be used for the clients. Let's assume that `A` and `B` will
be the member machines, and `C` will be the client machine.
- There should be 2 members per machine with the total of 4 members.
(2 members on `A`, 2 members on `B`).
- All 4 members should be able to discover each other and form a
4-member cluster.
- On the client machine (`C`), there should be 10 clients. There 
shouldn't be any members on this machine.
- On each of the clients, there must be at most 32 active operations
at any time. The client test program should execute the following API
calls during the duration of the test.
  - Put/Gets
  - Predicates
  - Map Listeners
  - Entry Processors
- The test programs should run for 48 hours.
- At the end of the 48 hours, the followings must be verified:
  - All the client processes are successfully started and shut down.
  - Logs produced by the clients/members should not contain any errors
  printed.

# Usage

- Clone the repository to all 3 of the machines.
- Put the `hazelcast-${VERSION}.jar` and `hazelcast-${VERSION}-tests.jar`
to this folder for the member machines. 
  - You can upload those JARs from your computer using `scp` or download
  JARs from `https://repo1.maven.org/maven2/com/hazelcast/hazelcast/${VERSION}/`.
- Make sure the versions of the JARs and the version written in 
`start_members.sh` are the same.
- Run `start_members.sh` on 2 member machines. The script will start
2 members.
- Check the member logs outputted to `member_logs/hazelcast-out-[0|1]`
and make sure that
  - They are using the `hazelcast.xml` in this folder.
  - They can form a 4-member cluster.
    - If they cannot form a cluster, play with the `join` config
    in the `hazelcast.xml`. It will try to use multicast discovery
    by default, which might be disabled on certain environments.
    In such cases, using `tcp-ip` discovery by setting 
    `<multicast enabled="false">` and `<tcp-ip enabled="true">` in 
    the `hazelcast.xml` and providing member addresses would probably
    work.
- Run `start_clients.sh` in the client machine, which will start 10
clients.
  - Assuming one of your members is running on address `10.212.1.117`
  on port `5701`, you can start 48-hours long tests with the following
  command. You just need to provide the address of the one of the 
  members.
    ```bash
    ./start_clients.sh 10.212.1.117:5701
    ```
  - You can also control the duration of the tests by passing a second
  argument. It is a floating point number in the unit of hours. Its 
  default value is set to `48.0`, so you don't need to pass it most
  of the time. For example, if you want to run clients for 15 
  minutes, run the following command.
  ```bash
  ./start_clients.sh 10.212.1.117:5701 0.25 
  ```
- Check the client logs outputted to `client_logs/client-[0..10]`
and verify that, each client is connected to all 4 of the members
and started the SOAK test operations.
- Come back 48 hours later, and do the followings:
  - Clients should be automatically shutdown after 48 hours. Verify
  there is no client left running.
  - Collect client and member logs and analyze them. There should
  be no errors printed.
  - Manually shutdown the members.
  - If you have come this far without any problems, good for you! 
  You have completed the SOAK test process.
