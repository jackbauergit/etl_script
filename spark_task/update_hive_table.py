# coding=utf8

from __future__ import print_function
from pyspark.sql import SparkSession


if __name__ == "__main__":
    """
        Usage: pi [partitions]
    """
    hc = SparkSession\
        .builder\
        .appName("hello pyspark")\
        .enableHiveSupport()\
        .getOrCreate()

    df = hc.sql('show databases')
    df.show()

    hc.stop()
