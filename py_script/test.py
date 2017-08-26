from shell_client import ShellExecutor
import threading
#  import time


class Task(threading.Thread):
    def __init__(self, cmd):
        threading.Thread.__init__(self)
        self.cmd = cmd

    def run(self):
        se = ShellExecutor(self.cmd)
        se.execute()


ts = list()
for i in range(100000):
    #  cmd = ['/usr/bin/sleep', '10']
    cmd = ['ps', '-ef']
    t = Task(cmd)
    t.start()
    ts.append(t)
    #  time.sleep(1)

for t in ts:
    t.join()
