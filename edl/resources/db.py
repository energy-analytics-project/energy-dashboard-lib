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
(edc-cli) toddg@beast:~/proj/energy-dashboard$ edc feed data-oasis-atl-ruc-zone-map proc insert
{"ts":"09/18/2019 04:12:54 PM", "msg":{"name": "edl.resources.db", "src": "data-oasis-atl-ruc-zone-map", "method": "insert", "sql_dir": "/home/toddg/proj/energy-dashboard/data/data-oasis-atl-ruc-zone-map/sql", "db_dir": "/home/toddg/proj/energy-dashboard/data/data-oasis-atl-ruc-zone-map/db", "new_files": 99}}
Traceback (most recent call last):
  File "src/40_inse.py", line 98, in <module>
    run(logger, m, config())
  File "src/40_inse.py", line 78, in run
    state_file)
  File "/home/toddg/bin/anaconda3/envs/edc-cli/lib/python3.7/site-packages/edl/resources/state.py", line 22, in update
    for item in generator:
  File "/home/toddg/bin/anaconda3/envs/edc-cli/lib/python3.7/site-packages/edl/resources/db.py", line 34, in insert
    yield insert_file(logger, resource_name, sql_dir, db_dir, sql_file_name, idx, depth=0, max_depth=5)
  File "/home/toddg/bin/anaconda3/envs/edc-cli/lib/python3.7/site-packages/edl/resources/db.py", line 56, in insert_file
    with sqlite3.connect(os.path.join(db_dir, db_name)) as cnx:
sqlite3.OperationalError: unable to open database file
"""


import os
import logging
import sqlite3
from edl.resources import log

def insert(logger, resource_name, sql_dir, db_dir, new_files):
    chlogger = logger.getChild(__name__)
    new_files_count = len(new_files)
    if not os.path.exists(db_dir):
        os.makedirs(db_dir)
    if not os.path.exists(db_dir):
        raise Exception("Failed to create db_dir: %s" % db_dir)
    log.info(chlogger, {
        "name"      : __name__,
        "src"       : resource_name, 
        "method"    : "insert",
        "sql_dir"   : sql_dir,
        "db_dir"    : db_dir,
        "new_files" : new_files_count,
        })
    for (idx, sql_file_name) in enumerate(new_files):
        yield insert_file(logger, resource_name, sql_dir, db_dir, sql_file_name, idx, depth=0, max_depth=5)

def insert_file(logger, resource_name, sql_dir, db_dir, sql_file_name, idx, depth, max_depth):
    chlogger = logger.getChild(__name__)
    db_name = gen_db_name(resource_name, depth)
    sql_file = os.path.join(sql_dir, sql_file_name)
    if depth > max_depth:
        log.error(chlogger, {
            "name"      : __name__,
            "src"       : resource_name, 
            "method"    : "insert",
            "sql_dir"   : sql_dir,
            "db_dir"    : db_dir,
            "db_name"   : db_name,
            "file_idx"  : idx,
            "sql_file"  : sql_file,
            "depth"     : depth,
            "max_depth" : max_depth,
            "ERROR"     :"insert sql_file failed, max_depth exceeded",
            })
        return
        
    with sqlite3.connect(os.path.join(db_dir, db_name)) as cnx:
        try:
            with open(sql_file, 'r') as sf:
                log.info(chlogger, {
                    "name"      : __name__,
                    "src"       : resource_name, 
                    "method"    : "insert",
                    "sql_dir"   : sql_dir,
                    "db_dir"    : db_dir,
                    "db_name"   : db_name,
                    "file_idx"  : idx,
                    "sql_file"  : sql_file,
                    "depth"     : depth,
                    "message"   : "started",
                    })
                cnx.executescript(sf.read())
                log.info(chlogger, {
                    "name"      : __name__,
                    "src"       : resource_name, 
                    "method"    : "insert",
                    "sql_dir"   : sql_dir,
                    "db_dir"    : db_dir,
                    "db_name"   : db_name,
                    "file_idx"  : idx,
                    "sql_file"  : sql_file,
                    "depth"     : depth,
                    "message"   : "completed",
                    })
            return sql_file
        except Exception as e:
            log.error(chlogger, {
                "name"      : __name__,
                "src"       : resource_name, 
                "method"    : "insert",
                "sql_dir"   : sql_dir,
                "db_dir"    : db_dir,
                "db_name"   : db_name,
                "file_idx"  : idx,
                "sql_file"  : sql_file,
                "depth"     : depth,
                "ERROR":"insert sql_file failed",
                "exception": str(e),
                })
            insert_file(logger, resource_name, sql_dir, db_dir, sql_file, idx, depth+1, max_depth)

def gen_db_name(resource_name, depth):
    return "%s_%02d.db" % (resource_name, depth)
