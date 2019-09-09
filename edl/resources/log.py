import logging
import logging.config
import os

LOG_LEVELS=[
                "CRITICAL",
                "ERROR",
                "WARNING",
                "INFO",
                "DEBUG"
        ]

def configure_logging():
    if os.path.exists("logging.conf"):
        logging.config.fileConfig('logging.conf')
    else:
        logging.basicConfig(
                format='{"ts":"%(asctime)s", "msg":%(message)s}', 
                datefmt='%m/%d/%Y %I:%M:%S %p')
