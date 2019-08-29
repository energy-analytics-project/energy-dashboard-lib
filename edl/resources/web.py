"""
web.py : download resources from a URL
"""

import requests
import os
import time
import logging
from edl.resources import filesystem

def generate_urls(date_pairs, url_template, date_format="%Y%m%d"):
    """
    Generator for download urls:
    date_pairs      : list of tuples with (start, end) dates
    url_template    : contains a _START_ and _END_ strings which will be replaced
                      by (start,end) tuples formated by the date_format
    date_format     : format string fro the start and end dates
    """
    for (start, end) in date_pairs:
        s = start.strftime(date_format)
        e = end.strftime(date_format)
        yield url_template.replace("_START_", s).replace("_END_", e)


def download(resource_name, delay, urls, state_file, path):
    """
    urls        : list of urls to download
    state_file  : list of urls that have already been downloaded
    path        : path to write downloaded files to
    """
    downloaded = []
    prev_downloaded = set()
    if os.path.exists(state_file):
        with open(state_file, "r") as f:
            prev_downloaded = set([line.rstrip() for line in f])
    for url in urls:
        try:
            filename = filesystem.url2filename(url)
            if url in prev_downloaded:
                logging.info({ "src":resource_name, "action":'skip_download', "url":url, "file":filename, "msg":'url exists in download manifest'})
                continue
            if os.path.exists(filename):
                logging.info({ "src":resource_name, "action":'skip_download', "url":url, "file":filename, "msg":'file exists locally, updating manifest'})
                # update the state_file with files that were found on disk
                downloaded.append(url)
                continue
            # url does not exist in manifest and the file does not exist on disk, download it
            r = requests.get(url)
            if r.status_code == 200:
                with open(os.path.join(path, filename), 'wb') as fd:
                    for chunk in r.iter_content(chunk_size=128):
                        fd.write(chunk)
                downloaded.append(url)
                logging.info({ "src":resource_name, "action":'download', "url":url, "file":filename})
            else:
                logging.error({ "src":resource_name, "action":'download', "url":url, "file":filename, "status_code":r.status_code, "error":'http_reuqest_failed'})
        except Exception as e:
            logging.error({ "src":resource_name, "action":'download', "url":url, "error":e})
        time.sleep(delay)
    return downloaded
