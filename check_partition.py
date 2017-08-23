# -*- coding: utf-8 -*-

import re
import datetime
import threading
from logger import logger_etl as logger
from collections import OrderedDict
from shell_client import LocalBeelineExecutor as LocalExecutor


src_db_name = 'mysql'
dest_db_name = 'commodity'
check_by_col = 'modified'
partition_col = 'created'
partition_name = 'dt'
test_mode = False
max_concurrency_num = 10


def _get_executor(stmt):
    thrift_ip = '10.189.245.201'
    thrift_port = 10000
    thrift_user = 'root'
    return LocalExecutor(stmt, thrift_ip, thrift_port, thrift_user)


def check_hive_table(tbl_name, begin_date):
    logger.debug('begin check')
    check_src_tbl_names = _load_checkd_src_tbl_info(tbl_name, begin_date)
    logger.debug(u'表 %s 更新了 %d 个分表: %s' % (
        tbl_name, len(check_src_tbl_names), check_src_tbl_names.keys()))

    logger.debug(check_src_tbl_names)
    partition_collector = OrderedDict()
    for _, pts in check_src_tbl_names.iteritems():
        for pt in pts:
            partition_collector[pt] = 1

    all_partitions = partition_collector.keys()
    all_partitions.sort()
    if not all_partitions:
        return

    logger.debug(u'表 %s 需要更新 %s 个分区，时间跨度从 %s 到 %s' % (
        tbl_name, len(all_partitions), all_partitions[0], all_partitions[-1]))


def _splice_table_cols(tbl_name):
    query_stmt = "SHOW COLUMNS FROM %s" % tbl_name
    lhe = _get_executor(query_stmt)
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


class QueryTask(threading.Thread):
    def __init__(self, stmt, semaphore):
        threading.Thread.__init__(self)
        self.stmt = stmt
        self.semaphore = semaphore
        self.result = list()

    def run(self):
        try:
            self.real_exec()

        except Exception as e:
            logger.error(e, exc_info=True)

        finally:
            self.semaphore.release()

    def real_exec(self):
        lhe = _get_executor(self.stmt)
        self.result = lhe.execute()

    def get_result(self):
        return self.result


def _load_checkd_src_tbl_info(tbl_name, begin_date):
    begin_date_str = begin_date.strftime('%Y-%m-%d %H:%M:%S')
    end_date_str = _get_after_day(begin_date)
    src_tbl_names = _load_src_tbl_names(tbl_name)
    logger.debug(u'和表 %s 相关的分表总共有 %s 个' % (
        tbl_name, len(src_tbl_names)))
    need_check_partitions = OrderedDict()
    semaphore = threading.Semaphore(max_concurrency_num)

    work_threads = list()
    for stn in src_tbl_names:
        query_stmt = (
            "SELECT DISTINCT substring(%s, 0, 10) AS %s FROM %s.%s "
            "WHERE %s>='%s' and %s<='%s'") % (
                partition_col, partition_col, src_db_name, stn,
                check_by_col, begin_date_str,
                check_by_col, end_date_str)
        qt = QueryTask(query_stmt, semaphore)
        qt.start()

    for wt in work_threads:
        wt.join()
        rows = wt.get_result()
        logger.debug(rows)

        if rows:
            partitions = need_check_partitions.get(stn, {})
            if not partitions:
                partitions = OrderedDict()
            for val in rows:
                if partition_name in val:
                    continue
                partitions[val] = 1

            need_check_partitions[stn] = partitions

    return need_check_partitions


def _load_src_tbl_names(tbl_name):
    logger.debug(tbl_name)
    if test_mode:
        return ['product_ext_with_no_partition']

    stmt = 'show tables from %s' % src_db_name
    lhe = _get_executor(stmt)
    rows = lhe.execute()

    pattern = tbl_name + r'(_\d+)?$'

    logger.debug(pattern)
    releated_tbl_names = list()
    for row in rows:
        tn = row.split('|')[1]
        tn = tn.strip()
        if re.search(pattern, tn):
            releated_tbl_names.append(tn)

    logger.debug(releated_tbl_names)
    return releated_tbl_names


def _get_after_day(curr_date, gap_days=1):
    after_date = curr_date + datetime.timedelta(days=gap_days)
    return after_date.strftime('%Y-%m-%d %H:%M:%S')


def _real_main():
    tbl_name = 'product'
    check_date = '2017-08-21'

    check_date = datetime.datetime.strptime(check_date, '%Y-%m-%d')
    check_hive_table(tbl_name, check_date)


if __name__ == '__main__':
    _real_main()
