import os
import logging
import sqlite3

def insert(resource_name, db_dir, db_name, ddl_create, sql_insert, filename_entry_tuples):
    cnx = initdb(resource_name, os.path.join(db_dir, db_name), ddl_create)
    with cnx:
        for (filename, entries) in filename_entry_tuples:
            try:
                cnx.executemany(sql_insert, entries)
                logging.info({
                    "src":resource_name, 
                    "action":"insert",
                    "file":filename,
                    "succeeded":len(entries),
                    })
                yield filename
            except Exception as ex:
                logging.error({
                    "src":resource_name, 
                    "action":"insert",
                    "error":ex,
                    "filename":filename,
                    "msg":"insert failed"
                    })

def initdb(resource_name, db, ddl_create):
    logging.debug({
        "src":resource_name, 
        "action":"initdb",
        "db_path":db
        })
    try:
        conn = sqlite3.connect(db)
        c = conn.cursor()
        c.execute(ddl_create)
        conn.commit()
        return conn
    except Exception as e:
        logging.error({
            "src":resource_name, 
            "action":"initdb",
            "db_path":db,
            "error":e,
            "msg":"failed to open database"
            })
