#!/bin/sh

HAZELCAST_TEST_VERSION="4.0.3"
HAZELCAST_VERSION="4.0.3"
PID=$$

mkdir -p "server_logs"

CLASSPATH="hazelcast-${HAZELCAST_VERSION}.jar:hazelcast-${HAZELCAST_TEST_VERSION}-tests.jar"
CMD_CONFIGS="-Djava.net.preferIPv4Stack=true"
java "${CMD_CONFIGS}" -cp ${CLASSPATH} \
    com.hazelcast.core.server.HazelcastMemberStarter \
    > server_logs/hazelcast-${HAZELCAST_VERSION}-${PID}-out.log 2>server_logs/hazelcast-${HAZELCAST_VERSION}-${PID}-err.log &
