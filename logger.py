# coding=utf-8

import logging
import logging.config

logging.config.fileConfig('logger.conf')

logger_mongodb_etl = logging.getLogger("mongodbEtl")
