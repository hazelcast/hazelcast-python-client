#!/bin/sh

if [ "$1" = "--local" ] ; then
    USER="--user"
else
    USER=""
fi

HAZELCAST_VERSION="3.6.3"
HAZELCAST_RC_VERSION="0.1-SNAPSHOT"
SNAPSHOT_REPO="https://oss.sonatype.org/content/repositories/snapshots"
RELEASE_REPO="http://repo1.maven.apache.org/maven2"
#ENTERPRISE_REPO="https://repository-hazelcast-l337.forge.cloudbees.com/release/"


mvn dependency:get -DrepoUrl=${SNAPSHOT_REPO} -Dartifact=com.hazelcast:hazelcast-remote-controller:${HAZELCAST_RC_VERSION} -Ddest=hazelcast-remote-controller-${HAZELCAST_RC_VERSION}.jar
mvn dependency:get -DrepoUrl=${RELEASE_REPO} -Dartifact=com.hazelcast:hazelcast:${HAZELCAST_VERSION} -Ddest=hazelcast-${HAZELCAST_VERSION}.jar

pip install -r test-requirements.txt ${USER}

java -cp hazelcast-remote-controller-${HAZELCAST_RC_VERSION}.jar:hazelcast-${HAZELCAST_VERSION}.jar  com.hazelcast.remotecontroller.Main
