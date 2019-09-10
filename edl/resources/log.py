import logging
import logging.config
import os
import json
import sys

LOG_LEVELS=[
                "CRITICAL",
                "ERROR",
                "WARNING",
                "INFO",
                "DEBUG"
        ]

def configure_logging(logging_level=None):
    logging.basicConfig(
            format='{"ts":"%(asctime)s", "msg":%(message)s}', 
            datefmt='%m/%d/%Y %I:%M:%S %p')
    if logging_level is not None:
        logging.basicConfig(level=logging_level)
    elif os.path.exists("logging.conf"):
        logging.config.fileConfig('logging.conf')

def debug(logger, obj):
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(json.dumps(obj))
def info(logger, obj):
    if logger.isEnabledFor(logging.INFO):
        logger.info(json.dumps(obj))
def warning(logger, obj):
    if logger.isEnabledFor(logging.WARNING):
        logger.warning(json.dumps(obj))
def error(logger, obj):
    if logger.isEnabledFor(logging.ERROR):
        logger.error(json.dumps(obj))
def critical(logger, obj):
    if logger.isEnabledFor(logging.CRITICAL):
        logger.critical(json.dumps(obj))
    sys.exit(1)
