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

import os
import logging
import sqlite3
from edl.resources import log
from edl.resources import filesystem

class MemDb():
    def __init__(self, db_path):
        self.db_path = db_path
        self.filedb = None
        self.memdb = None
    def open(self):
        # source
        source = self.filedb = sqlite3.connect(self.db_path)
        # dest
        dest = self.memdb = sqlite3.connect(':memory:')
        # backup
        source.backup(dest)
        return dest
    def close(self):
        # source
        source = self.memdb
        # dest
        dest = self.filedb
        # backup
        source.backup(dest)
        return dest
    def db(self):
        return self.memdb
    def __repr__(self):
        return str(self.db_path)

class DbMgr():
    def __init__(self, logger, resource_name):
        self.dbs = {}
        self.resource_name = resource_name
        self.logger = logger
    def get(self, db_path):
        log.debug(self.logger, {
            "name"      : __name__,
            "src"       : self.resource_name,
            "method"    : "DbMgr.get",
            "db_path"   : db_path,
            })
        if db_path not in self.dbs:
            db = MemDb(db_path)
            cnx = db.open()
            self.dbs[db_path] = db
            log.debug(self.logger, {
                "name"      : __name__,
                "src"       : self.resource_name,
                "method"    : "DbMgr.get",
                "db_path"   : db_path,
                "message"   : "Created in memory db",
                })
        return self.dbs[db_path].db()
    def __enter__(self):
        return self
    def __exit__(self, type, value, traceback):
        for k,v in self.dbs.items():
            v.close()
            log.debug(self.logger, {
                "name"      : __name__,
                "src"       : self.resource_name,
                "method"    : "DbMgr.__exit__",
                "db_path"   : k,
                "message"   : "Closed in memory db",
                })

    def __repr__(self):
        return str(self.dbs.keys())

def insert(logger, resource_name, sql_dir, db_dir, new_files):
    chlogger = logger.getChild(__name__)
    with DbMgr(chlogger, resource_name) as dbmgr:
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
            yield insert_file(logger, resource_name, dbmgr, sql_dir, db_dir, sql_file_name, idx, depth=0, max_depth=5)
       
        save_dir        = os.path.join(os.path.dirname(db_dir), "save")
        save_state_file = os.path.join(save_dir, "state.txt")
        db_files        = filesystem.glob_dir(db_dir, ".db")
        with open(save_state_file, 'w') as f:
            for dbf in db_files:
                f.write("%s\n" % dbf)
                log.debug(logger, {
                    "name"      : __name__,
                    "method"    : "insert",
                    "resource"  : resource_name,
                    "db_file"   : dbf,
                    "state_file": save_state_file,
                    "message"   : "added db_file to state file",
                    })

def insert_file(logger, resource_name, dbmgr, sql_dir, db_dir, sql_file_name, idx, depth, max_depth):
    chlogger    = logger.getChild(__name__)
    db_name     = gen_db_name(resource_name, depth)
    sql_file    = os.path.join(sql_dir, sql_file_name)
    db_file     = os.path.join(db_dir, db_name)
    if depth > max_depth:
        log.error(chlogger, {
            "name"      : __name__,
            "src"       : resource_name,
            "method"    : "insert_file",
            "db_file"   : db_file,
            "file_idx"  : idx,
            "sql_file"  : sql_file,
            "depth"     : depth,
            "max_depth" : max_depth,
            "dbmgr"     : str(dbmgr),
            "ERROR"     :"insert sql_file failed, max_depth exceeded",
            })
        return

    log.info(chlogger, {
        "name"      : __name__,
        "src"       : resource_name,
        "method"    : "insert_file",
        "db_file"   : db_file,
        "file_idx"  : idx,
        "sql_file"  : sql_file,
        "depth"     : depth,
        "dbmgr"     : str(dbmgr),
        "message"   : "started",
        })
        
    cnx = dbmgr.get(db_file)
    try:
        with open(sql_file, 'r') as sf:
            log.debug(chlogger, {
                "name"      : __name__,
                "src"       : resource_name,
                "method"    : "insert_file",
                "db_file"   : db_file,
                "file_idx"  : idx,
                "sql_file"  : sql_file,
                "depth"     : depth,
                "dbmgr"     : str(dbmgr),
                "message"   : "started",
                })
            cnx.executescript(sf.read())
            log.debug(chlogger, {
                "name"      : __name__,
                "src"       : resource_name,
                "method"    : "insert_file",
                "db_file"   : db_file,
                "file_idx"  : idx,
                "sql_file"  : sql_file,
                "depth"     : depth,
                "dbmgr"     : str(dbmgr),
                "message"   : "completed",
                })
        return sql_file_name
    except Exception as e:
        log.error(chlogger, {
            "name"      : __name__,
            "src"       : resource_name,
            "method"    : "insert_file",
            "file_idx"  : idx,
            "db_file"   : db_file,
            "sql_file"  : sql_file,
            "depth"     : depth,
            "dbmgr"     : str(dbmgr),
            "ERROR"     : "insert sql_file failed",
            "exception": str(e),
            })
        insert_file(logger, resource_name, dbmgr, sql_dir, db_dir, sql_file, idx, depth+1, max_depth)

def gen_db_name(resource_name, depth):
    return "%s_%02d.db" % (resource_name, depth)
