# -*- coding: utf-8 -*-

from logger import logger_etl as logger
from shell_client import LocalHiveExecutor


def _real_main():
    logger.debug('hello')
    lhe = LocalHiveExecutor()
    rows = lhe.execute('show tables from source')
    logger.debug(rows)
    pass


if __name__ == '__main__':
    _real_main()
