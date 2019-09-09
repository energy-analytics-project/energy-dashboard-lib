import logging

LOG_LEVELS=[
                "CRITICAL",
                "ERROR",
                "WARNING",
                "INFO",
                "DEBUG",
                "NOTSET"
        ]

def configure_logging_format():
    logging.basicConfig(
            format='{"ts":%(asctime)s, "msg":%(message)s}', 
            datefmt='%m/%d/%Y %I:%M:%S %p')
