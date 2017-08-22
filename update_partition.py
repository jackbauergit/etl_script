# -*- coding: utf-8 -*-

import re
import datetime
from logger import logger_etl as logger
from collections import OrderedDict
#  from shell_client import LocalHiveExecutor as LocalExecutor
from shell_client import LocalBeelineExecutor as LocalExecutor
from config import src_db_name, dest_db_name, test_mode


update_by_col = 'modified'
partition_col = 'created'
partition_name = 'dt'


def update_hive_table(tbl_name, begin_date):
    logger.debug('begin update')
    dest_tbl_name = '%s.%s_with_1_partition' % (dest_db_name, tbl_name)
    update_cols = _splice_table_cols(dest_tbl_name)
    update_src_tbl_names = _load_updated_src_tbl_info(tbl_name, begin_date)

    _clean_expired_partitions(update_src_tbl_names, dest_tbl_name)
    for stn, pts in update_src_tbl_names.iteritems():
        _update_hive(stn, dest_tbl_name, update_cols, pts)


def _clean_expired_partitions(partitions_in_tbl, dest_tbl_name):
    partition_collector = OrderedDict()
    for _, pts in partitions_in_tbl.iteritems():
        for pt in pts:
            partition_collector[pt] = 1

    clean_pts = partition_collector.keys()
    for pt in clean_pts:
        clean_stmt = (
            "ALTER TABLE %s DROP PARTITION (%s='%s')") % (
                dest_tbl_name, partition_name, pt)
        lhe = LocalExecutor(clean_stmt)
        result = lhe.execute()
        logger.debug(result)


def _splice_table_cols(tbl_name):
    query_stmt = "SHOW COLUMNS FROM %s" % tbl_name
    lhe = LocalExecutor(query_stmt)
    rows = lhe.execute()
    cols = list()
    tbl_filter = [partition_name]
    for val in rows:
        if not val:
            continue

        if val in tbl_filter:
            continue

        cols.append(val)

    return ', '.join(cols)


def _load_updated_src_tbl_info(tbl_name, begin_date):
    begin_date_str = begin_date.strftime('%Y-%m-%d %H:%M:%S')
    end_date_str = _get_after_day(begin_date)
    src_tbl_names = _load_src_tbl_names(tbl_name)
    need_update_partitions = OrderedDict()
    for stn in src_tbl_names:
        query_stmt = (
            "SELECT DISTINCT substring(%s, 0, 10) AS %s FROM %s.%s "
            "WHERE %s>='%s' and %s<='%s'") % (
                partition_col, partition_col, src_db_name, stn,
                update_by_col, begin_date_str,
                update_by_col, end_date_str)
        lhe = LocalExecutor(query_stmt)
        rows = lhe.execute()
        logger.debug(rows)

        if rows:
            partitions = need_update_partitions.get(stn, {})
            if not partitions:
                partitions = OrderedDict()
            for val in rows:
                if partition_name in val:
                    continue
                partitions[val] = 1

            need_update_partitions[stn] = partitions

    return need_update_partitions


def _load_src_tbl_names(tbl_name):
    logger.debug(tbl_name)
    if test_mode:
        return ['product_ext_with_no_partition']

    stmt = 'show tables from %s' % src_db_name
    lhe = LocalExecutor(stmt)
    rows = lhe.execute()

    pattern = tbl_name + r'(_\d+)?$'

    logger.debug(pattern)
    releated_tbl_names = [tn for tn in rows if re.search(pattern, tn)]
    logger.debug(releated_tbl_names)
    return releated_tbl_names


def _update_hive(
        src_tbl_name, dest_tbl_name, update_cols, partitions, gap_days=1):
    #  begin_date_str = begin_date.strftime('%Y-%m-%d %H:%M:%S')
    #  end_date_str = _get_after_day(begin_date, gap_days)
    #  pts = ["'%s'" % pt for pt in partitions]
    #  pt_str = ', '.join(pts)
    for pt in partitions:
        update_stmt = (
            "INSERT INTO TABLE %s PARTITION (%s) "
            "SELECT %s, substring(%s, 0, 10) as %s FROM %s.%s "
            "WHERE %s LIKE '%s%%'") % (
                dest_tbl_name, partition_name, update_cols, partition_col,
                partition_name, src_db_name, src_tbl_name,
                partition_col, pt)
        lhe = LocalExecutor(update_stmt)
        result = lhe.execute()
        logger.debug(result)


def _get_after_day(curr_date, gap_days=1):
    after_date = curr_date + datetime.timedelta(days=gap_days)
    return after_date.strftime('%Y-%m-%d %H:%M:%S')


def _real_main():
    tbl_name = 'product_ext'
    update_date = datetime.datetime.strptime('2014-12-05', '%Y-%m-%d')
    update_hive_table(tbl_name, update_date)
    pass


if __name__ == '__main__':
    _real_main()
