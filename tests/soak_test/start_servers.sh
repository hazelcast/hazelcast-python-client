#!/bin/bash

VERSION="5.0"

mkdir -p "server_logs"

CLASSPATH="hazelcast-${VERSION}.jar:hazelcast-${VERSION}-tests.jar"
CMD_CONFIGS="-Djava.net.preferIPv4Stack=true"

for i in {0..1}
do
    java "${CMD_CONFIGS}" -cp ${CLASSPATH} \
        com.hazelcast.core.server.HazelcastMemberStarter \
        1> server_logs/hazelcast-err-"$i" \
        2> server_logs/hazelcast-out-"$i" &
done
