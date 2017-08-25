# -*- coding: utf-8 -*-
"""ssh client"""


import subprocess
#  import commands
import popen2
#  import os
#  from logger import logger_etl as logger
from config import thrift_ip, thrift_port, thrift_user


spark_home = '/export/servers/spark-2.2.0-bin-hadoop2.6'


class LocalBeelineExecutor():
    def __init__(
            self, stmt, ip=thrift_ip, port=thrift_port, user=thrift_user):
        self.stmt = stmt
        self.ip = ip
        self.port = port
        self.user = user

    def execute(self):
        quote_stmt = '''"%s"''' % self.stmt
        url = "-ujdbc:hive2://%s:%s" % (self.ip, self.port)
        beeline_cmd = "%s/bin/beeline" % spark_home
        cmd = [
            beeline_cmd, url, '-n%s' % self.user,
            '--showHeader=false',
            '--showWarnings=false',
            '-e', quote_stmt]
        cmd = ' '.join(cmd)
        print(cmd)
        pipe = popen2.Popen3(cmd, True)
        to_child = pipe.tochild
        from_child = pipe.fromchild
        child_err = pipe.childerr
        #  if data:
        #  to_child.write(data)
        to_child.close()
        out = err = ''
        while pipe.poll() is None:
            out += from_child.read()
            err += child_err.read()
        out += from_child.read()
        err += child_err.read()
        status = pipe.wait()
        #  sp = subprocess.Popen(cmd, stderr=None, stdout=None, shell=False)
        #  out, err = sp.communicate()

        #  out = subprocess.check_output(cmd, stderr=None)
        #  status, out = commands.getstatusoutput(''.join(cmd))
        #  pout, pin, perr = popen2.popen3(cmd)
        #  err = perr.readlines()
        #  beeline_rows = pout.readlines()
        #  pout.close()
        #  pin.close()
        #  perr.close()
        #  out = ''
        #  err = ''
        #  print(err)
        print(out)
        print(status)
        beeline_rows = _clean_result(out)
        result_collector = dict()
        for row in beeline_rows:
            row = row.strip('-+| ')
            if not row:
                continue

            result_collector[row] = 1

        return result_collector.keys()


def _clean_result(raw_result):
    rows = raw_result.split('\n')
    clean_rows = list()
    for row in rows:
        row = row.strip()
        if not row:
            continue
        clean_rows.append(row)

    return clean_rows


class LocalHiveExecutor():
    def __init__(self, stmt):
        self.stmt = stmt

    def execute(self):
        quote_stmt = '''"%s"''' % self.stmt
        cmd = ['hive', '-S', '-e', quote_stmt]
        print(cmd)
        #  sp = subprocess.Popen(
        #  cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=False)
        #  out, err = sp.communicate()
        #  print(out)
        #  print(err)
        out = ""
        return _clean_result(out)


class ShellExecutor():
    def __init__(self, stmt):
        self.stmt = stmt

    def execute(self):
        print(self.stmt)
        sp = subprocess.Popen(
            self.stmt, stderr=subprocess.PIPE, stdout=subprocess.PIPE,
            shell=False)
        out, err = sp.communicate()
        print(out)
        print(err)
        if out:
            out = out.strip()
        if err:
            err = err.strip()
        return out, err
