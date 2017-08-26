# -*- coding: utf-8 -*-

import logging
import logging.config


log_path = "etl.log"


logger_etl = None


def _get_logger():
    file_logger = logging.getLogger('etl')
    file_logger.setLevel(logging.DEBUG)
    file_handler = logging.handlers.TimedRotatingFileHandler(log_path, 'D')
    file_handler.setLevel(logging.DEBUG)
    #  file_handler.backupCount = 20
    fmt = "%(asctime)s-%(filename)s#%(lineno)d-%(levelname)s: %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt, datefmt)

    file_handler.setFormatter(formatter)
    file_logger.addHandler(file_handler)

    return file_logger


if logger_etl is None:
    logger_etl = _get_logger()
