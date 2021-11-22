import os
import subprocess
import sys
from os.path import isfile

SERVER_VERSION = "5.0"
RC_VERSION = "0.8-SNAPSHOT"

RELEASE_REPO = "https://repo1.maven.apache.org/maven2"
ENTERPRISE_RELEASE_REPO = "https://repository.hazelcast.com/release/"
SNAPSHOT_REPO = "https://oss.sonatype.org/content/repositories/snapshots"
ENTERPRISE_SNAPSHOT_REPO = "https://repository.hazelcast.com/snapshot/"

if SERVER_VERSION.endswith("-SNAPSHOT"):
    REPO = SNAPSHOT_REPO
    ENTERPRISE_REPO = ENTERPRISE_SNAPSHOT_REPO
else:
    REPO = RELEASE_REPO
    ENTERPRISE_REPO = ENTERPRISE_RELEASE_REPO

if RC_VERSION.endswith("-SNAPSHOT"):
    RC_REPO = SNAPSHOT_REPO
else:
    RC_REPO = RELEASE_REPO

IS_ON_WINDOWS = os.name == "nt"
CLASS_PATH_SEPARATOR = ";" if IS_ON_WINDOWS else ":"


def download_if_necessary(repo, artifact_id, version, is_test_artifact=False):
    dest_file_name = artifact_id + "-" + version
    if is_test_artifact:
        dest_file_name += "-tests"
    dest_file_name += ".jar"

    if isfile(dest_file_name):
        print("Not downloading %s, because it already exists." % dest_file_name)
        return dest_file_name

    print("Downloading " + dest_file_name)

    artifact = "com.hazelcast:" + artifact_id + ":" + version
    if is_test_artifact:
        artifact += ":jar:tests"

    args = [
        "mvn",
        "-q",
        "dependency:get",
        "-Dtransitive=false",
        "-DrepoUrl=" + repo,
        "-Dartifact=" + artifact,
        "-Ddest=" + dest_file_name,
    ]

    process = subprocess.run(args, shell=IS_ON_WINDOWS)
    if process.returncode != 0:
        print("Failed to download " + dest_file_name)
        sys.exit(1)

    return dest_file_name


def start_rc(stdout=None, stderr=None):
    artifacts = []

    rc = download_if_necessary(RC_REPO, "hazelcast-remote-controller", RC_VERSION)
    tests = download_if_necessary(REPO, "hazelcast", SERVER_VERSION, True)
    sql = download_if_necessary(REPO, "hazelcast-sql", SERVER_VERSION)

    artifacts.extend([rc, tests, sql])

    enterprise_key = os.environ.get("HAZELCAST_ENTERPRISE_KEY", None)

    if enterprise_key:
        server = download_if_necessary(ENTERPRISE_REPO, "hazelcast-enterprise", SERVER_VERSION)
        ep_tests = download_if_necessary(
            ENTERPRISE_REPO, "hazelcast-enterprise", SERVER_VERSION, True
        )

        artifacts.append(server)
        artifacts.append(ep_tests)
    else:
        server = download_if_necessary(REPO, "hazelcast", SERVER_VERSION)
        artifacts.append(server)

    class_path = CLASS_PATH_SEPARATOR.join(artifacts)

    args = [
        "java",
        "-cp",
        class_path,
        "com.hazelcast.remotecontroller.Main",
        "--use-simple-server",
    ]

    if enterprise_key:
        args.insert(1, "-Dhazelcast.enterprise.license.key=" + enterprise_key)

    return subprocess.Popen(args=args, stdout=stdout, stderr=stderr, shell=IS_ON_WINDOWS)


if __name__ == "__main__":
    rc_process = start_rc()
    try:
        rc_process.wait()
    except:
        rc_process.kill()
        rc_process.wait()
        raise
