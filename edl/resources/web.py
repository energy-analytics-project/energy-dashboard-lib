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

"""
web.py : download resources from a URL
"""

from edl.resources import filesystem
from edl.resources import log
from stat import S_IREAD, S_IRGRP, S_IROTH
import logging
import os
import pdb
import requests
import time
import traceback

def generate_urls(logger, date_pairs, url_template, date_format="%Y%m%d"):
    """
    Generate download urls for the provided date_pairs.

    date_pairs      : list of tuples with (start, end) dates
    url_template    : contains a _START_ and _END_ strings which will be replaced
                      by (start,end) tuples formated by the date_format
    date_format     : format string for the start and end dates

    TODO/BIKESHED   : replace _X_ with Jinja mustache templates {{}}
    """
    chlogger = logger.getChild(__name__)
    for (start, end) in date_pairs:
        s = start.strftime(date_format)
        e = end.strftime(date_format)
        url = url_template.replace("_START_", s).replace("_END_", e)
        log.debug(chlogger, {
            "name"      : __name__,
            "method"    : "generate_urls",
            "start"     : s,
            "end"       : e,
            "url"       : url
            })
        yield url



def download(logger, resource_name, delay, urls, state_file, path):
    """
    urls        : list of urls to download
    state_file  : list of urls that have already been downloaded
    path        : path to write downloaded files to
    """
    chlogger = logger.getChild(__name__)
    downloaded = []
    prev_downloaded = set()
    if os.path.exists(state_file):
        with open(state_file, "r") as f:
            prev_downloaded = set([line.rstrip() for line in f])
    for url in urls:
        try:
            filename = filesystem.url2filename(url)
            if url in prev_downloaded:
                log.debug(chlogger, {"src":resource_name, "action":'skip_download', "url":url, "file":filename, "msg":'url exists in download manifest'})
                continue
            target_file = os.path.join(path, filename)
            if os.path.exists(target_file):
                log.debug(chlogger, {"src":resource_name, "action":'skip_download', "url":url, "file":filename, "msg":'file exists locally, updating manifest'})
                # update the state_file with files that were found on disk
                downloaded.append(url)
                continue
            r = requests.get(url)
            if r.status_code == 200:
                with open(target_file, 'wb') as fd:
                    for chunk in r.iter_content(chunk_size=128):
                        fd.write(chunk)
                downloaded.append(url)
                # prevent file from being deleted
                log.info(chlogger, {"src":resource_name, "action":'download', "url":url, "file":filename})
            else:
                log.error(chlogger, {"src":resource_name, "action":'download', "url":url, "file":filename, "status_code":r.status_code, "ERROR":'http_request_failed'})
        except Exception as e:
            log.error(chlogger, {"src":resource_name, "action":'download', "url":url, "ERROR": "http_request_failed", "exception" : str(e), "traceback": str(tb = traceback.format_exc())})
        
        # TODO: this is such a hack
        time.sleep(delay)
        # ensure that all files in the download directery are read only
        for f in filesystem.glob_dir(path, ".zip"):
            os.chmod(os.path.join(path, f), S_IREAD|S_IRGRP|S_IROTH)
    return downloaded
