import logging
import subprocess

try:
    output = subprocess.check_output(["git", "show", "-s", "--format=\"%h\""]).decode()
    commit_id = output.strip().replace("\"", "").replace("'", "")
except:
    commit_id = ""

logging.basicConfig(
    format='%(asctime)s%(msecs)03d [' + commit_id + '][%(threadName)s][%(name)s] %(levelname)s: %(message)s',
    datefmt="%H:%M:%S,")
logging.getLogger().setLevel(logging.INFO)
