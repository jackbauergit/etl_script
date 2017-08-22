# -*- coding: utf-8 -*-
"""ssh client"""


import subprocess
import os
from logger import logger_etl as logger
from config import thrift_ip, thrift_port


class LocalBeelineExecutor():
    def __init__(self, stmt):
        self.stmt = stmt

    def execute(self):
        quote_stmt = '''"%s"''' % self.stmt
        url = "-ujdbc:hive2://%s:%s" % (thrift_ip, thrift_port)
        beeline_cmd = "%s/bin/beeline" % os.environ.get('SPARK_HOME')
        cmd = [beeline_cmd, url, '-nroot', '-e', quote_stmt]
        logger.debug(cmd)
        sp = subprocess.Popen(
            cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=False)
        out, err = sp.communicate()
        #  logger.debug(out)
        logger.debug(err)
        return _clean_result(out)


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
        sp = subprocess.Popen(
            cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, shell=False)
        out, err = sp.communicate()
        #  logger.debug(out)
        logger.debug(err)
        return _clean_result(out)


class ShellExecutor():
    def __init__(self, stmt):
        self.stmt = stmt

    def execute(self):
        logger.debug(self.stmt)
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
