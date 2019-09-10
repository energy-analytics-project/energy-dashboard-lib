#! /usr/bin/env python3

# -----------------------------------------------------------------------------
# 30_pars.py : parse resources from an xml file into SQL for later insertion
# -----------------------------------------------------------------------------

from edl.resources import log
from edl.resources import state
from edl.resources import xmlparser
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
    config = {
            "source_dir"    : os.path.join(cwd, "xml"),
            "working_dir"   : os.path.join(cwd, "sql"),
            "state_file"    : os.path.join(cwd, "sql", "parsed.txt")
            }
    return config


# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------
def run(manifest, config, logging_level):
    log.configure_logging(logging_level)
    resource_name   = manifest['name']
    pk_exclusions   = manifest.pop('pk_exclusions', ['value'])
    xml_namespace   = manifest['xml_namespace']
    xml_dir         = config['source_dir']
    sql_dir         = config['working_dir']
    state_file      = config['state_file']
    new_files = state.new_files(resource_name, state_file, xml_dir, '.xml')
    state.update(
            xmlparser.parse(resource_name, new_files, xml_dir, sql_dir, pk_exclusions, xml_namespace), 
            state_file)

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) > 1:
        loglevel = sys.argv[1]
    else:
        loglevel = "INFO"
    with open('manifest.json', 'r') as json_file:
        m = json.load(json_file)
        run(m, config(), logging_level=loglevel)