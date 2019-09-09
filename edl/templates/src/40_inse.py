#! /usr/bin/env python3

# -----------------------------------------------------------------------------
# 30_inse.py : parse resources from an xml file and insert into database
# -----------------------------------------------------------------------------

import os
import logging
import xml.dom.minidom as md
import pprint
import datetime as dt
import sqlite3
import json
from edl.resources import state
from edl.resources import log
from edl.resources import db
from edl.resources import xml

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
    xml_dir                 = os.path.join(cwd, "xml")
    db_dir                  = os.path.join(cwd, "db")
    state_file              = os.path.join(db_dir, "inserted.txt")
    config = {
            "source_dir"    : xml_dir,
            "working_dir"   : db_dir,
            "state_file"    : state_file
            }
    return config


# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------
def run(manifest, config, logging_level=logging.INFO):
    log.configure_logging(logging_level)
    resource_name   = manifest['name']
    sql_insert      = " ".join(manifest['sql_insert'])
    ddl_create      = " ".join(manifest['ddl_create'])
    xml_dir         = config['source_dir']
    db_dir          = config['working_dir']
    state_file      = config['state_file']
    db_name         = "%s.db" % resource_name
    state.update(
            db.insert(
                resource_name,
                db_dir,
                db_name,
                ddl_create,
                sql_insert,
                xml.parse(
                    resource_name,
                    xml.new_xml_files(resource_name, state_file, xml_dir),
                    xml_dir,
                    xml2maps)),
            state_file)

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    with open('manifest.json', 'r') as json_file:
        m = json.load(json_file)
        run(m, config())
