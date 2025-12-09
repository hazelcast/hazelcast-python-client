#!/bin/bash

VERSION="5.6.0"

mkdir -p "member_logs"

declare -a pids

cleanup () {
  for pid in "${pids[@]}"; do
    echo "Stopping $pid"
    kill "$pid"
  done
}

trap cleanup EXIT

CLASSPATH="hazelcast-${VERSION}.jar:hazelcast-${VERSION}-tests.jar"
CMD_CONFIGS="-Djava.net.preferIPv4Stack=true"

for i in {0..1}
do
    java "${CMD_CONFIGS}" -cp ${CLASSPATH} \
        com.hazelcast.core.server.HazelcastMemberStarter \
        1> member_logs/hazelcast-err-"$i" \
        2> member_logs/hazelcast-out-"$i" &
    pid=$!
    echo "$pid running"
    pids+=("$pid")
done

wait
