import logging
import subprocess

from hazelcast import __version__, HazelcastClient

from hazelcast.util import calculate_version

try:
    output = subprocess.check_output(["git", "show", "-s", '--format="%h"']).decode()
    commit_id = output.strip().replace('"', "").replace("'", "")
except:
    commit_id = ""

logging.basicConfig(
    format="%(asctime)s%(msecs)03d ["
    + commit_id
    + "][%(threadName)s][%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S,",
)
logging.getLogger().setLevel(logging.INFO)

if calculate_version(__version__) < calculate_version("4.1"):
    # For the 4.0 version of the client, there is a known
    # bug that might affect client shutdown in some scheduling
    # scenarios. See
    # https://github.com/hazelcast/hazelcast-python-client/issues/378
    # and linked PRs for details. In short, shutdown might fail
    # on connection_manager shutdown, due to unexpected pending
    # connections. What we do here is that, we try to shutdown
    # as usual, if it fails, we try to shutdown leftovers and
    # return without raising.

    original_shutdown = HazelcastClient.shutdown

    def patched_shutdown(self):
        try:
            # business as usual
            original_shutdown(self)
        except RuntimeError:
            # ConnectionManager shutdown failed.

            # We need to explicitly shutdown invocation
            # service due to infinitely running cleanup
            # timer.
            self._invocation_service.shutdown()

            # Shutdown reactor thread so that it won't
            # affect other tests
            self._reactor.shutdown()

    HazelcastClient.shutdown = patched_shutdown
