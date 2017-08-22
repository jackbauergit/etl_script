# -*- coding: utf-8 -*-
"""ssh client"""


import subprocess
from logger import logger_etl as logger


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
        rows = [row.strip() for row in out.split('\n') if row.strip()]
        return rows


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
