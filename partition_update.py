# -*- coding: utf-8 -*-

import re
import datetime
from logger import logger_etl as logger
from shell_client import LocalHiveExecutor
from config import src_db_name, dest_db_name, test_mode


update_by_col = 'modified'


def update_hive_table(tbl_name, begin_date):
    logger.debug('begin update')
    dest_tbl_name = '%s_with_1_partition' % tbl_name
    update_cols = _splice_table_cols('%s.%s' % (dest_db_name, dest_tbl_name))
    src_tbl_names = _load_src_tbl_names(tbl_name)
    for stn in src_tbl_names:
        _update_hive(
            stn, dest_tbl_name, update_cols, begin_date)


def _splice_table_cols(tbl_name):
    query_stmt = "desc %s" % tbl_name
    lhe = LocalHiveExecutor(query_stmt)
    rows = lhe.execute()
    cols = list()
    for row in rows:
        if not row:
            continue

        val = row.split(' ')[0]
        if val == update_by_col:
            continue

        cols.append(val)

    return ', '.join(cols)


def _update_hive(
        src_tbl_name, dest_tbl_name, update_cols, begin_date, gap_days=1):
    end_date = begin_date + datetime.timedelta(days=gap_days)
    begin_date_str = begin_date.strftime('%Y-%m-%d %H:%M:%S')
    end_date_str = end_date.strftime('%Y-%m-%d %H:%M:%S')
    update_stmt = (
        "INSERT OVERWRITE TABLE %s.%s PARTITION (dt) "
        "SELECT %s, substring(%s, 0, 10) as dt from %s.%s "
        "WHERE %s>='%s' and %s<='%s'") % (
            dest_db_name, dest_tbl_name, update_cols, update_by_col,
            src_db_name, src_tbl_name, update_by_col, begin_date_str,
            update_by_col, end_date_str)
    lhe = LocalHiveExecutor(update_stmt)
    lhe.execute()
    pass


def _load_src_tbl_names(tbl_name):
    if test_mode:
        return ['product_ext_with_no_partition']

    stmt = 'show tables from %s' % dest_db_name
    lhe = LocalHiveExecutor(stmt)
    rows = lhe.execute()

    pattern = tbl_name + r'(_\d+)?$'

    logger.debug(pattern)
    releated_tbl_names = [tn for tn in rows if re.search(pattern, tn)]
    logger.debug(tbl_name)
    logger.debug(releated_tbl_names)
    return releated_tbl_names


def _real_main():
    tbl_name = 'product_ext'
    update_date = datetime.datetime.strptime('2014-12-05', '%Y-%m-%d')
    update_hive_table(tbl_name, update_date)
    pass


if __name__ == '__main__':
    _real_main()
