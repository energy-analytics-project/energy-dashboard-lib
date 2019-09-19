# edl : common library for the energy-dashboard tool-chain
# Copyright (C) 2019  Todd Greenwood-Geer (Enviro Software Solutions, LLC)
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import json
import logging
import logging.config
import os
import sys
import traceback

LOGGING_LEVEL_STRINGS = {
            50 : "CRITICAL",
            40 : "ERROR",
            30 : "WARNING",
            20 : "INFO",
            10 : "DEBUG", 
            0  : "NOTSET"
        }

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
        tb = traceback.format_exc()
        print(tb)
    sys.exit(1)
