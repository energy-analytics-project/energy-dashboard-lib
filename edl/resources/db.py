import os
import logging
import sqlite3
from edl.resources import log


def insert(logger, resource_name, sql_dir, db_dir, db_name, new_files):
    chlogger = logger.getChild(__name__)
    with sqlite3.connect(os.path.join(db_dir, db_name)) as cnx:
        for new_file in new_files:
            sqlfile = os.path.join(sql_dir, new_file) 
            try:
                with open(sqlfile, 'r') as sf:
                    cnx.executescript(sf.read())
                    log.info(chlogger, {
                        "name"  : __name__,
                        "src"   :resource_name, 
                        "method":"insert",
                        "file"  :sqlfile,
                        })
                yield sqlfile
            except Exception as e:
                log.error(chlogger, {
                    "name"  : __name__,
                    "src"   :resource_name, 
                    "method":"insert",
                    "file"  :sqlfile,
                    "ERROR":"insert failed",
                    "exception": str(e)
                    })
