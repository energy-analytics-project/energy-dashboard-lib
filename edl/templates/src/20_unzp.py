#! /usr/bin/env python3

# -----------------------------------------------------------------------------
# 20_unzp.py : unzip zip files in ZIP_DIR to XML_DIR in preparation for
#              xml parsing and further injestion
# -----------------------------------------------------------------------------

import os
import zipfile as zf
import logging
import json
from edl.resources import state
from edl.resources import log
from edl.resources import zp

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
def config():
    """
    config = {
            "source_dir"    : location of zip files
            "working_dir"   : location to unzip xml files to
            "state_file"    : fqpath to a file that lists unzipped files
            }
    """
    cwd                     = os.path.abspath(os.path.curdir)
    zip_dir                 = os.path.join(cwd, "zip")
    xml_dir                 = os.path.join(cwd, "xml")
    state_file              = os.path.join(xml_dir, "unzipped.txt")
    config = {
            "source_dir"    : zip_dir,
            "working_dir"   : xml_dir,
            "state_file"    : state_file
            }
    return config



# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------
def run(manifest, config, logging_level=logging.INFO):
    log.configure_logging(logging_level)
    resource_name   = manifest['name']
    xml_dir         = config['working_dir']
    zip_dir         = config['source_dir']
    state_file      = config['state_file']
    new_files = state.new_files(resource_name, state_file, zip_dir, '.zip')
    state.update(
            zp.unzip(
                resource_name,
                new_files,
                zip_dir,
                xml_dir),
            state_file)


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    with open('manifest.json', 'r') as json_file:
        m = json.load(json_file)
        run(m, config())
