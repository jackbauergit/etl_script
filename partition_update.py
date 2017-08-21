# -*- coding: utf-8 -*-

import re
from logger import logger_etl as logger
from shell_client import LocalHiveExecutor


def update_hive_table(tbl_name):
    logger.debug('hello')
    stmt = 'show tables from source'
    lhe = LocalHiveExecutor(stmt)
    rows = lhe.execute()

    pattern = tbl_name + r'\d*$'

    logger.debug(pattern)
    releated_tbl_names = [tn for tn in rows if re.search(pattern, tn)]
    logger.debug(releated_tbl_names)
    pass


def _real_main():
    tbl_name = 'product_ext'
    logger.debug(tbl_name)
    update_hive_table(tbl_name)
    pass


if __name__ == '__main__':
    _real_main()
