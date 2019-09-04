#!/usr/bin/env python3

import re
from enum import Enum
import datetime as dt
import fileinput
import json
import logging
import os
import pdb
import pprint
import sys
import xml.dom.minidom as md
import xmltodict

class SqlTypeEnum(Enum):
    """
    NULL. The value is a NULL value.
    INTEGER. The value is a signed integer, stored in 1, 2, 3, 4, 6, or 8 bytes depending on the magnitude of the value.
    REAL. The value is a floating point value, stored as an 8-byte IEEE floating point number.
    TEXT. The value is a text string, stored using the database encoding (UTF-8, UTF-16BE or UTF-16LE).
    BLOB. The value is a blob of data, stored exactly as it was input.

    https://www.sqlite.org/datatype3.html
    """
    NULL        = 1
    INTEGER     = 2
    REAL        = 3
    TEXT        = 4
    BLOB        = 5

    def type_of(element):
        """
        Return the SqlType for the element.
        """
        if element is None:
            return SqlTypeEnum.NULL
        try:
            int(element)
            return SqlTypeEnum.INTEGER
        except:
            try:
                float(element)
                return SqlTypeEnum.REAL
            except:
                try:
                    str(element)
                    return SqlTypeEnum.TEXT
                except:
                    return SqlTypeEnum.BLOB

def sql_type_str(e):
    if e == SqlTypeEnum.NULL: return "NULL"
    if e == SqlTypeEnum.INTEGER: return "INTEGER"
    if e == SqlTypeEnum.REAL: return "REAL"
    if e == SqlTypeEnum.TEXT: return "TEXT"
    if e == SqlTypeEnum.BLOB: return "BLOB"

class Flattener():
    def __init__(self, xmlfile):
        self.xmlfile    = xmlfile
        self.json       = None

    def parse(self):
        """
        Internal method to parse the xmlfile into a dom at self.dom

        Example:

            $ ./flub.py  20180224_20180225_AS_MILEAGE_CALC_N_20190809_08_27_07_v1.xml | jq '.'

            {
              "OASISReport": {
                "@xmlns": "http://www.caiso.com/soa/OASISReport_v1.xsd",
                "MessageHeader": {
                  "TimeDate": "2019-08-09T15:27:07-00:00",
                  "Source": "OASIS",
                  "Version": "v20131201"
                },
                "MessagePayload": {
                  "RTO": {
                    "name": "CAISO",
                    "REPORT_ITEM": [
                      {
                        "REPORT_HEADER": {
                          "SYSTEM": "OASIS",
                          "TZ": "PPT",
                          "REPORT": "AS_MILEAGE_CALC",
                          "UOM": "MW",
                          "INTERVAL": "ENDING",
                          "SEC_PER_INTERVAL": "3600"
                        },
                        "REPORT_DATA": {
                          "DATA_ITEM": "RMD_AVG_MIL",
                          "RESOURCE_NAME": "AS_CAISO_EXP",
                          "OPR_DATE": "2018-02-24",
                          "INTERVAL_NUM": "24",
                          "INTERVAL_START_GMT": "2018-02-25T07:00:00-00:00",
                          "INTERVAL_END_GMT": "2018-02-25T08:00:00-00:00",
                          "VALUE": "2426.9"
                        }
                      },

        """
        self.json = xmltodict.parse(self.xmlfile.read())
        return self

    def to_json(self):
        return json.dumps(self.json)

    def tables_and_types(self):
        return recursive_tables(self.json, ('root'), [], {}, {'id': SqlTypeEnum.INTEGER})

def clean(s):
    s2 = re.sub('[^a-zA-Z0-9_]', '', s).lower()
    return s2

def recursive_tables(obj, xname="", path=[], tables={}, types={}, tablemap={}):
    name = clean(xname)
    if isinstance(obj, dict):
        tables[name] = [clean(k) for k in obj.keys()]
        tablemap[name] = path
        for k, v in obj.items():
            (tables, types, tablemap) = recursive_tables(v, clean(k), path + [name], tables, types, tablemap)
    elif isinstance(obj, list):
        for idx, item in enumerate(obj):
            (tables, types, tablemap) = recursive_tables(item, name, path + [name], tables, types, tablemap)
    else:
        if name not in types:
            types[name] = SqlTypeEnum.type_of(obj)
    return (tables, types, tablemap)

def recursive_iter(obj, keys=()):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from recursive_iter(v, keys + (k,))
    elif any(isinstance(obj, t) for t in (list, tuple)):
        for idx, item in enumerate(obj):
            yield from recursive_iter(item, keys + (idx,))
    else:
        yield keys, obj

def gen_primary_key(table_name, tables, types, primary_key_exclusions):
    cols = tables[table_name]
    primary_key = [c for c in cols if c not in primary_key_exclusions and c in types]
    return primary_key


def find_parent_table(t, tablemap, tables):
    #print("%s : %s" % (t, tablemap[t]))
    if t == 'root':
        return None
    path = tablemap[t]
    parent = path.pop()
    while parent == t:
        parent = path.pop()
    if parent == 'root':
        return None
    return parent


def generate_sql_ddl(tables, types, tablemap, primary_key_exclusions):
    del tables['root']
    for tbl,cols in tables.items():
        cols = [c for c in cols if c in types] 
        if len(cols) == 0:
            # insert a synthetic column
            cols = cols + ['id']
            tables[tbl] = cols
        if len(cols) > 0:
            foreign_keys        = []
            fk_column_types     = []
            column_types        = []
            ftbl = find_parent_table(tbl, tablemap, tables)
            if ftbl != None:
                f_primary_keys = gen_primary_key(ftbl, tables, types, primary_key_exclusions)
                fkeys = ["%s_%s" % (ftbl, x) for x in f_primary_keys] 
                foreign_keys.append("FOREIGN KEY (%s) REFERENCES %s(%s)" % (", ".join(fkeys), ftbl, ", ".join(f_primary_keys)))
                for fpk in f_primary_keys:
                    fk_column_types.append("%s_%s %s" % (ftbl, fpk, sql_type_str(types[fpk])))
            for col in cols:
                if col in types:
                    column_types.append("%s %s" % (col, sql_type_str(types[col])))
                    
            primary_key_def = ", ".join(gen_primary_key(tbl, tables, types, primary_key_exclusions))
            column_types.extend(fk_column_types)
            column_types.extend(foreign_keys)
            combined_key_def = ", ".join(column_types)
            #ddl = "PRAGMA foreign_keys = ON;"
            ddl = ""
            ddl += "CREATE TABLE IF NOT EXISTS %s (%s, PRIMARY KEY (%s));" % (tbl, combined_key_def, primary_key_def)
            print(ddl) 





if __name__ == "__main__":
    infile = sys.argv[1]
    with open(infile, 'r') as fh:
        f = Flattener(fh).parse()
        (tables, types, tablemap) = f.tables_and_types()
        generate_sql_ddl(tables, types, tablemap, ['value'])
