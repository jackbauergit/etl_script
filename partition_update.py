# -*- coding: utf-8 -*-

from logger import logger_etl as logger
from shell_client import LocalHiveExecutor


def _real_main():
    logger.debug('hello')
    stmt = 'show tables from source'
    lhe = LocalHiveExecutor(stmt)
    rows = lhe.execute()
    logger.debug(rows)
    pass


if __name__ == '__main__':
    _real_main()
