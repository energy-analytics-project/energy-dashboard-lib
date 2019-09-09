#! /usr/bin/env python3

# -----------------------------------------------------------------------------
# 10_down.py : download resources from resource_url
# 
# * after downloading, resource urls are appended to zip/downloaded.txt
# * zip/downloaded.txt can be checked into the repo, whereas the the downloaded
#   resources should not be checked in to git. Instead, they are uploaded to
#   an S3 bucket 'eap'.
# -----------------------------------------------------------------------------

import datetime
import requests
import sys
import os
import time
import logging
import json
from edl.resources import state
from edl.resources import log
from edl.resources import web
from edl.resources import time as xtime


# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
def config():
    """
    config = {
            "source_dir"    : not used
            "working_dir"   : location to download zip files
            "state_file"    : fqpath to file that lists downloaded zip files
            }
    """
    cwd                     = os.path.abspath(os.path.curdir)
    zip_dir                 = os.path.join(cwd, "zip")
    state_file              = os.path.join(zip_dir, "state.txt")
    config = {
            "working_dir"   : zip_dir,
            "state_file"    : state_file
            }
    return config

# -----------------------------------------------------------------------------
# Entrypoint
# -----------------------------------------------------------------------------
def run(logger, manifest, config):
    start_date      = datetime.date(*manifest['start_date'])
    resource_name   = manifest['name']
    resource_url    = manifest['url']
    delay           = manifest['download_delay_secs']
    download_dir    = config['working_dir']
    state_file      = config['state_file']
    # sleep for N seconds in between downloads to meet caiso expected use requirements
    dates   = xtime.range_pairs(xtime.day_range_to_today(start_date))
    urls    = list(web.generate_urls(logger, dates, resource_url))
    log.info(logger, {
        "name"      : __name__,
        "method"    : "run",
        "resource"  : resource_name,
        "url"       : resource_url,
        "download_dir": download_dir,
        "state_file": state_file,
        "start_date": str(start_date),
        "urls_count": len(urls),
        })
    state.update(
            web.download(
                logger,
                resource_name,
                delay,
                urls,
                state_file,
                download_dir),
            state_file
            )

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
        "src"       : "10_down.py"
        })
    with open('manifest.json', 'r') as json_file:
        m = json.load(json_file)
        run(logger, m, config())
