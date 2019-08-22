import logging

def configure_logging(logging_level=logging.INFO):
    logging.basicConfig(
            format='{"ts":%(asctime)s, "msg":%(message)s}', 
            datefmt='%m/%d/%Y %I:%M:%S %p', level=logging_level)
