import os
import logging
import sqlite3
from edl.resources import log

def insert(logger, resource_name, sql_dir, db_dir, new_files):
    chlogger = logger.getChild(__name__)
    new_files_count = len(new_files)
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
