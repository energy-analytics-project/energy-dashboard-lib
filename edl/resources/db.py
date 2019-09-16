import os
import logging
import sqlite3
from edl.resources import log


def insert(logger, resource_name, sql_dir, db_dir, db_name, new_files):
    chlogger = logger.getChild(__name__)
    new_files_count = len(new_files)
    log.info(chlogger, {
        "name"      : __name__,
        "src"       : resource_name, 
        "method"    : "insert",
        "sql_dir"   : sql_dir,
        "db_dir"    : db_dir,
        "db_name"   : db_name,
        "new_files" : new_files_count,
        })
    with sqlite3.connect(os.path.join(db_dir, db_name)) as cnx:
        for (idx, new_file) in enumerate(new_files):
            sqlfile = os.path.join(sql_dir, new_file) 
            try:
                with open(sqlfile, 'r') as sf:
                    log.info(chlogger, {
                        "name"      : __name__,
                        "src"       : resource_name, 
                        "method"    : "insert",
                        "sql_dir"   : sql_dir,
                        "db_dir"    : db_dir,
                        "db_name"   : db_name,
                        "new_files" : new_files_count,
                        "file_idx"  : idx,
                        "sql_file"  : sqlfile,
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
                        "new_files" : new_files_count,
                        "file_idx"  : idx,
                        "sql_file"  : sqlfile,
                        "message"   : "completed",
                        })
                yield sqlfile
            except Exception as e:
                log.error(chlogger, {
                    "name"      : __name__,
                    "src"       : resource_name, 
                    "method"    : "insert",
                    "sql_dir"   : sql_dir,
                    "db_dir"    : db_dir,
                    "db_name"   : db_name,
                    "new_files" : new_files_count,
                    "file_idx"  : idx,
                    "sql_file"  : sqlfile,
                    "ERROR":"insert of sql_file failed",
                    "exception": str(e),
                    })
