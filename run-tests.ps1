$HazelcastVersion="3.8.2-SNAPSHOT"

$HazelcastRcVersion="0.2-SNAPSHOT"
$SnapshotRepo="https://oss.sonatype.org/content/repositories/snapshots"
$ReleaseRepo="http://repo1.maven.apache.org/maven2"

mvn dependency:get "-DrepoUrl=$SnapshotRepo" "-Dartifact=com.hazelcast:hazelcast-remote-controller:$HazelcastRcVersion" "-Ddest=hazelcast-remote-controller-$HazelcastRcVersion.jar"
mvn dependency:get "-DrepoUrl=$SnapshotRepo" "-Dartifact=com.hazelcast:hazelcast:$HazelcastVersion" "-Ddest=hazelcast-$HazelcastVersion.jar"

pip install -r test-requirements.txt --user

Write-Host Starting Hazelcast ...
Start-Process -FilePath javaw -ArgumentList ( "-Dhazelcast.enterprise.license.key=$HAZELCAST_ENTERPRISE_KEY","-cp", "hazelcast-remote-controller-$HazelcastRcVersion.jar;hazelcast-$HazelcastVersion.jar", "com.hazelcast.remotecontroller.Main" ) -RedirectStandardOutput "rc_stdout.txt" -RedirectStandardError "rc_stderr.txt"

Write-Host Wait for Hazelcast to start ...
Start-Sleep -s 15

Write-Host Running tests ...
python -m nose -v --with-xunit --cover-xml --cover-package=hazelcast --cover-inclusive --nologcapture

