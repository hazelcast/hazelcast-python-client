import sys
import threading
import time
from os.path import dirname
from codec_bench import Bench

sys.path.append(dirname(dirname(__file__)))

THREAD_COUNT = 2
ENTRY_COUNT = 10 * 1000
VALUE_SIZE = 10000


def do_benchmark():
    class ClientThread(threading.Thread):
        def __init__(self, name):
            threading.Thread.__init__(self, name=name)
            self.ops = 0
            self.decode = 0
            self.setDaemon(True)
            self.bench = Bench()

        def run(self):
            while True:
                self.bench.encode()
                self.bench.decode()

                self.ops += 1

    threads = [ClientThread("client-thread-%d" % i) for i in xrange(0, THREAD_COUNT)]
    for t in threads:
        t.start()

    start = time.time()
    counter = 1
    while counter < 10:
        time.sleep(5)
        print "ops per second : " + \
              str(sum([t.ops for t in threads]) / (time.time() - start))
        # for t in threads:
        #     print ("%s: ops: %d " % (t.name, t.ops))
        counter += 1


if __name__ == '__main__':
    do_benchmark()
