# coding=utf-8

import logging
import logging.config

logging.config.fileConfig('logger.conf')

logger_etl = logging.getLogger("etl")
