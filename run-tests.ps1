$serverVersion = "3.12-SNAPSHOT"

$hazelcastTestVersion=$serverVersion
$hazelcastEnterpriseTestVersion=$serverVersion
$hazelcastVersion=$serverVersion
$hazelcastEnterpriseVersion=$serverVersion
$hazelcastRCVersion="0.3-SNAPSHOT"
$snapshotRepo="https://oss.sonatype.org/content/repositories/snapshots"
$releaseRepo="http://repo1.maven.apache.org/maven2"
$enterpriseReleaseRepo="https://repository.hazelcast.com/release/"
$enterpriseSnapshotRepo="https://repository.hazelcast.com/snapshot/"

if ($serverVersion.Contains("SNAPSHOT")) {
    $repo=$snapshotRepo
    $enterpriseRepo=$enterpriseSnapshotRepo
} else {
    $repo=$releaseRepo
    $enterpriseRepo=$enterpriseReleaseRepo
}

if(Test-Path "hazelcast-remote-controller-${hazelcastRCVersion}.jar") {
	Write-Host "remote controller already exist, not downloading from maven."
} else {
	Write-Host "Downloading hazelcast-remote-controller-${hazelcastRCVersion}.jar ..."
    & "mvn" -q "dependency:get" "-DrepoUrl=${snapshotRepo}" "-Dartifact=com.hazelcast:hazelcast-remote-controller:${hazelcastRCVersion}" "-Ddest=hazelcast-remote-controller-${hazelcastRCVersion}.jar"
}

if(Test-Path "hazelcast-${hazelcastTestVersion}-tests.jar") {
	Write-Host "hazelcast-${hazelcastTestVersion}-tests.jar already exist, not downloading from maven."
} else {
	Write-Host "Downloading hazelcast-${hazelcastTestVersion}-tests.jar ..."
    & "mvn" -q "dependency:get" "-DrepoUrl=${repo}" "-Dartifact=com.hazelcast:hazelcast:${hazelcastTestVersion}:jar:tests" "-Ddest=hazelcast-${hazelcastTestVersion}-tests.jar"
}

[string]$classpath="hazelcast-remote-controller-${hazelcastRCVersion}.jar;hazelcast-${hazelcastTestVersion}-tests.jar;"

if(Test-Path env:HAZELCAST_ENTERPRISE_KEY){
	if(Test-Path "hazelcast-enterprise-${hazelcastEnterpriseVersion}.jar") {
		Write-Host "hazelcast-enterprise-${hazelcastEnterpriseVersion}.jar already exist, not downloading from maven."
	} else {
		Write-Host "Downloading hazelcast-enterprise-${hazelcastEnterpriseVersion}.jar ..."
		& "mvn" -q "dependency:get" "-DrepoUrl=${enterpriseRepo}" "-Dartifact=com.hazelcast:hazelcast-enterprise:${hazelcastEnterpriseVersion}" "-Ddest=hazelcast-enterprise-${hazelcastEnterpriseVersion}.jar"
	}

	if(Test-Path "hazelcast-enterprise-${hazelcastEnterpriseTestVersion}-tests.jar") {
		Write-Host "hazelcast-enterprise-${hazelcastEnterpriseTestVersion}-tests.jar already exist, not downloading from maven."
	} else {
		Write-Host "Downloading hazelcast-enterprise-${hazelcastEnterpriseTestVersion}-tests.jar ..."
		& "mvn" -q "dependency:get" "-DrepoUrl=${enterpriseRepo}" "-Dartifact=com.hazelcast:hazelcast-enterprise:${hazelcastEnterpriseTestVersion}:jar:tests" "-Ddest=hazelcast-enterprise-${hazelcastEnterpriseTestVersion}-tests.jar"
	}
	$classpath += "hazelcast-enterprise-${hazelcastEnterpriseVersion}.jar;hazelcast-enterprise-${hazelcastEnterpriseTestVersion}-tests.jar"
} else{
	if(Test-Path "hazelcast-${hazelcastVersion}.jar") {
		Write-Host "hazelcast-${hazelcastVersion}.jar already exist, not downloading from maven."
	} else {
		Write-Host "Downloading hazelcast-${hazelcastVersion}.jar ..."
		& "mvn" -q "dependency:get" "-DrepoUrl=${repo}" "-Dartifact=com.hazelcast:hazelcast:${hazelcastVersion}" "-Ddest=hazelcast-${hazelcastVersion}.jar"
	}
	$classpath += "hazelcast-${hazelcastVersion}.jar"
}

pip install -r test-requirements.txt --user

Write-Host Starting Hazelcast ...
$remoteControllerApp = Start-Process -FilePath java -ArgumentList ( "-Dhazelcast.enterprise.license.key=$env:HAZELCAST_ENTERPRISE_KEY","-cp", "$classpath", "com.hazelcast.remotecontroller.Main" ) -RedirectStandardOutput "rc_stdout.log" -RedirectStandardError "rc_stderr.log" -PassThru

Write-Host Wait for Hazelcast to start ...
Start-Sleep -s 15

Write-Host Running tests ...
python -m nose -v --with-xunit --cover-xml --cover-package=hazelcast --cover-inclusive --nologcapture

Stop-Process -Force -Id $remoteControllerApp.Id
