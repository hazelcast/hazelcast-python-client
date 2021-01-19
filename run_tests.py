import os
import socket
import sys
import time
from contextlib import closing

import nose

from start_rc import start_rc


def wait_until_rc_is_ready():
    timeout = 300 + time.time()
    while time.time() < timeout:
        with closing(socket.socket()) as sock:
            if sock.connect_ex(("localhost", 9701)) == 0:
                return
            print("Remote controller is not ready yet. Sleeping 1 second.")
            time.sleep(1)

    raise Exception("Remote controller failed to start.")


if __name__ == "__main__":
    with open("rc_stdout.log", "w") as rc_stdout:
        with open("rc_stderr.log", "w") as rc_stderr:
            rc_process = start_rc(rc_stdout, rc_stderr)
            try:
                wait_until_rc_is_ready()
                args = [
                    __file__,
                    "-v",
                    "--with-xunit",
                    "--with-coverage",
                    "--cover-xml",
                    "--cover-package=hazelcast",
                    "--cover-inclusive",
                    "--nologcapture",
                ]

                is_oss = "HAZELCAST_ENTERPRISE_KEY" not in os.environ
                if is_oss:
                    args.extend(["-A", "not enterprise"])

                return_code = nose.run_exit(argv=args)
                rc_process.kill()
                rc_process.wait()
                sys.exit(return_code)
            except:
                rc_process.kill()
                rc_process.wait()
                raise
