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

def initdb(resource_name, fqp_db, ddl_create):
    logging.debug({
        "src":resource_name, 
        "action":"initdb",
        "db_path":db_path
        })
    try:
        conn = sqlite3.connect(fqp_db)
        c = conn.cursor()
        c.execute(ddl_create)
        conn.commit()
        return conn
    except Exception as e:
        logging.debug({
            "src":resource_name, 
            "action":"initdb",
            "db_path":fqp_db,
            "error":e,
            "msg":"failed to open database"
            })
