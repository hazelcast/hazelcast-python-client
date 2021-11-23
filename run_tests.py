import os
import socket
import subprocess
import sys
import time
from contextlib import closing


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
                sys.stdout.flush()

                args = [
                    "pytest",
                    "--cov=hazelcast",
                    "--cov-report=xml",
                ]

                enterprise_key = os.environ.get("HAZELCAST_ENTERPRISE_KEY", None)
                if not enterprise_key:
                    args.extend(["-m", "not enterprise"])

                process = subprocess.run(args)
                rc_process.kill()
                rc_process.wait()
                sys.exit(process.returncode)
            except:
                rc_process.kill()
                rc_process.wait()
                raise
