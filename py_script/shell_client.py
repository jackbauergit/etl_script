# -*- coding: utf-8 -*-
"""ssh client"""


import subprocess
#  import popen2
from logger import logger_etl as logger
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
        #  cmd = ' '.join(cmd)
        logger.debug(cmd)
        sp = subprocess.Popen(
            cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=False)
        out, err = sp.communicate()

        logger.debug(err)
        logger.debug(out)
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
        logger.debug(cmd)
        #  sp = subprocess.Popen(
        #  cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=False)
        #  out, err = sp.communicate()
        #  logger.debug(out)
        #  logger.debug(err)
        out = ""
        return _clean_result(out)


class ShellExecutor():
    def __init__(self, stmt):
        self.stmt = stmt

    def execute(self):
        logger.debug(self.stmt)
        #  ()
        sp = subprocess.Popen(
            self.stmt, stderr=subprocess.PIPE, stdout=subprocess.PIPE,
            shell=False)
        out, err = sp.communicate()
        logger.debug(out)
        logger.debug(err)
        if out:
            out = out.strip()
        if err:
            err = err.strip()
        return out, err
