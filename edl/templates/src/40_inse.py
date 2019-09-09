#! /usr/bin/env python3

# -----------------------------------------------------------------------------
# 30_inse.py : parse resources from an xml file and insert into database
# -----------------------------------------------------------------------------

from edl.resources import db
from edl.resources import log
from edl.resources import state
import datetime as dt
import json
import logging
import os
import pprint
import sqlite3
import sys
import xml.dom.minidom as md

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
def config():
    """
    config = {
            "source_dir"    : location of the xml files
            "working_dir"   : location of the database
            "state_file"    : fqpath to file that lists the inserted xml files
            }
    """
    cwd                     = os.path.abspath(os.path.curdir)
    sql_dir                 = os.path.join(cwd, "sql")
    db_dir                  = os.path.join(cwd, "db")
    state_file              = os.path.join(db_dir, "state.txt")
    config = {
            "source_dir"    : sql_dir,
            "working_dir"   : db_dir,
            "state_file"    : state_file
            }
    return config


# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------
def run(logger, manifest, config):
    resource_name   = manifest['name']
    sql_dir         = config['source_dir']
    db_dir          = config['working_dir']
    state_file      = config['state_file']
    new_files = state.new_files(resource_name, state_file, sql_dir, '.sql')
    state.update(
                db.insert(logger, resource_name, sql_dir, db_dir, new_files),
                state_file)

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) > 1:
        loglevel = sys.argv[1]
    else:
        loglevel = "INFO"
    log.configure_logging()
    logger = logging.getLogger(__name__)
    logger.setLevel(loglevel)
    log.info(logger, {
        "name"      : __name__,
        "method"    : "main",
        "src"       : "40_inse.py"
        })
    with open('manifest.json', 'r') as json_file:
        m = json.load(json_file)
        run(logger, m, config())
