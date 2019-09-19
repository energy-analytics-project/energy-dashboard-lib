#!/usr/bin/env python3
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

import base64
import traceback
from enum import Enum
import codecs
import datetime as dt
import fileinput
import json
import logging
import os
import pdb
import pprint
import re
import sys
import uuid
#import xmltodict
from edl.resources import log
from edl.external import xmltodict
import sqlite3

class Walker(object):
    def __init__(self, logger, dict_handler_func=None, list_handler_func=None, item_handler_func=None):
        def default_dict_handler_func(stack):
            pass
        def default_list_handler_func(stack):
            pass
        def default_item_handler_func(stack):
            pass
        self.logger = logger
        self.dict_handler_func = dict_handler_func or default_dict_handler_func
        self.list_handler_func = list_handler_func or default_list_handler_func
        self.item_handler_func = item_handler_func or default_item_handler_func

    def walk(self, name, obj):
        self._walk([(name, obj)])

    def _walk(self, stack):
        """
        Walk object tree and inkoke handlers based on object type (dict, list, or item).
        """
        (name, obj) = stack[-1]
        if isinstance(obj, dict):
            self.dict_handler_func(stack)
            for k, v in obj.items():
                self._walk(stack + [(k, v)])
        elif isinstance(obj, list):
            self.list_handler_func(stack)
            for idx, item in enumerate(obj):
                self._walk(stack + [(idx, item)])
        else:
            self.item_handler_func(stack)
        stack.pop()

class SqlTypeEnum(Enum):
    """
    Convert datatypes found in XML into Sqlite3 datatypes:

        NULL. The value is a NULL value.
        INTEGER. The value is a signed integer, stored in 1, 2, 3, 4, 6, or 8 bytes depending on the magnitude of the value.
        REAL. The value is a floating point value, stored as an 8-byte IEEE floating point number.
        TEXT. The value is a text string, stored using the database encoding (UTF-8, UTF-16BE or UTF-16LE).
        BLOB. The value is a blob of data, stored exactly as it was input.

    See : https://www.sqlite.org/datatype3.html
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
    """
    Ideally this would hang of the enum class above, but making it __repr__ didn't work, strangely.
    TODO: 
    """
    if e == SqlTypeEnum.NULL: return "NULL"
    if e == SqlTypeEnum.INTEGER: return "INTEGER"
    if e == SqlTypeEnum.REAL: return "REAL"
    if e == SqlTypeEnum.TEXT: return "TEXT"
    if e == SqlTypeEnum.BLOB: return "BLOB"

class Table(object):
    def __init__(self, name, parent, exclusions):
        self.name = name
        """table name"""

        self.parent = parent
        """name parent of this table, e.g. where this table has foreign key references to"""

        self.exclusions = set(exclusions)
        """columns not to be included in the primary key"""
        
        self.columns = set()
        """columns in this table"""

        self.last_id = None
        """dirty hack to track last id of a list"""

    def primary_key(self):
        """mutable: the primary key (all the local columns minus the exclusions, if any)"""
        return list(self.columns - self.exclusions)

    def __repr__(self):
        return "name: %s, columns: %s, exclusions: %s, parent: %s, " % (self.name, self.columns, self.exclusions, self.parent)

class XML2SQLTransormer():
    """
    OASIS Reports have the following nested structure:

    Example:

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
            ...
        }

    Example transforms:

    # 1. Dict A maps to a list of anonymous dicts -> named dicts -> items, e.g. {A:[{T:{a:1,b:2,}},]}
    # 1. Dict A maps to a list of anonymous dicts -> items, e.g. {A:[{a:1,b:2,},]}
             => {A:[{T:{a:1,b:2,}},]}

   


    Example2:
    {
    "OASISReport": {
        "MessageHeader": {
            "Source": "OASIS",
            "TimeDate": "2019-09-07T14:50:16-00:00",
            "Version": "v20131201"
        },
        "MessagePayload": {
            "RTO": {
                "DISCLAIMER_ITEM": {
                    "DISCLAIMER": "The contents of these pages are subject to change without notice.  Decisions based on information contained within the California ISO's web site are the visitor's sole responsibility."
                },
                "REPORT_ITEM": [
                    {
                        "REPORT_DATA": [
                            {
                                "DATA_ITEM": "RMD_AVG_MIL",
                                "INTERVAL_END_GMT": "2019-09-05T08:00:00-00:00",
                                "INTERVAL_NUM": "1",
                                "INTERVAL_START_GMT": "2019-09-05T07:00:00-00:00",
                                "OPR_DATE": "2019-09-05",
                                "RESOURCE_NAME": "AS_CAISO_EXP",
                                "VALUE": "1750.69"
                            },

odict_keys(['DATA_ITEM', 'RESOURCE_NAME', 'OPR_DATE', 'INTERVAL_NUM', 'INTERVAL_START_GMT', 'INTERVAL_END_GMT', 'VALUE'])


                            {
                                "DATA_ITEM": "RMD_AVG_MIL",
                                "INTERVAL_END_GMT": "2019-09-05T09:00:00-00:00",
                                "INTERVAL_NUM": "2",
                                "INTERVAL_START_GMT": "2019-09-05T08:00:00-00:00",
                                "OPR_DATE": "2019-09-05",
                                "RESOURCE_NAME": "AS_CAISO_EXP",
                                "VALUE": "1506.88"
                            },


    Example:

        Running this tooling on an XML file generates the DDL create table statements necessary:

        $ ./energy-dashboard-lib/edl/resources/xmlparser.py 20180224_20180225_AS_MILEAGE_CALC_N_20190809_08_27_07_v1.xml

        CREATE TABLE IF NOT EXISTS oasisreport (xmlns TEXT, PRIMARY KEY (xmlns));
        CREATE TABLE IF NOT EXISTS messageheader (timedate TEXT, source TEXT, version TEXT, oasisreport_xmlns TEXT, FOREIGN KEY (oasisreport_xmlns) REFERENCES oasisreport(xmlns), PRIMARY KEY (timedate, source, version));
        CREATE TABLE IF NOT EXISTS messagepayload (id INTEGER, oasisreport_xmlns TEXT, FOREIGN KEY (oasisreport_xmlns) REFERENCES oasisreport(xmlns), PRIMARY KEY (id));
        CREATE TABLE IF NOT EXISTS rto (name TEXT, messagepayload_id INTEGER, FOREIGN KEY (messagepayload_id) REFERENCES messagepayload(id), PRIMARY KEY (name));
        CREATE TABLE IF NOT EXISTS report_item (id INTEGER, rto_name TEXT, FOREIGN KEY (rto_name) REFERENCES rto(name), PRIMARY KEY (id));
        CREATE TABLE IF NOT EXISTS report_header (system TEXT, tz TEXT, report TEXT, uom TEXT, interval TEXT, sec_per_interval INTEGER, report_item_id INTEGER, FOREIGN KEY (report_item_id) REFERENCES report_item(id), PRIMARY KEY (system, tz, report, uom, interval, sec_per_interval));
        CREATE TABLE IF NOT EXISTS report_data (data_item TEXT, resource_name TEXT, opr_date TEXT, interval_num INTEGER, interval_start_gmt TEXT, interval_end_gmt TEXT, value REAL, report_item_id INTEGER, FOREIGN KEY (report_item_id) REFERENCES report_item(id), PRIMARY KEY (data_item, resource_name, opr_date, interval_num, interval_start_gmt, interval_end_gmt));
        CREATE TABLE IF NOT EXISTS disclaimer_item (disclaimer TEXT, rto_name TEXT, FOREIGN KEY (rto_name) REFERENCES rto(name), PRIMARY KEY (disclaimer));


    Example:

        The output DDL can be quickly sanity checked by running it through sqlite3:

        $ python energy-dashboard-lib/edl/resources/xmlparser.py ./20180224_20180225_AS_MILEAGE_CALC_N_20190809_08_27_07_v1.xml | sqlite3

        No errors, so the structure is good.

    https://sqlite.org/foreignkeys.html
    https://docs.python.org/3/library/json.html
    """
    NULL_MIGRATION = set([SqlTypeEnum.REAL, SqlTypeEnum.INTEGER, SqlTypeEnum.TEXT, SqlTypeEnum.BLOB]) 
    BLOB_MIGRATION = set([SqlTypeEnum.REAL, SqlTypeEnum.INTEGER, SqlTypeEnum.TEXT]) 
    def __init__(self, logger, xmlfile):
        """
        Following 'Elegant Objects' style, constructor only sets vars. All
        work is delayed until needed.
        """
        self.logger             = logger
        """use log.info(self.logger, {})"""

        self.xmlfile            = xmlfile
        """xmlfile : file obect""" 

        self.json               = None
        """json : json object (typically a dict) resulting from parsing the xmlfile object"""

        self.sql_types          = {'id': SqlTypeEnum.TEXT}
        """sql_types : map : xml element name -> sqlite3 value type"""

        self.tables             = {}
        """tables : map : table name -> Table(), which are just table definitions for sql inserts"""

        self.root               = 'root'
        """root : name of the root node which is really the document root, for which no table is generated"""

    def parse(self):
        """
        Parse the loaded xmlfile and load into the self.json object.
        """
        self.json = xmltodict.parse(self.xmlfile.read(), process_namespaces=True, strip_namespaces=True)
        if self.logger.isEnabledFor(logging.DEBUG):
            print(json.dumps(self.json, indent=4, sort_keys=True))
        # allow method chaining
        return self

    def scan_all(self, exclusions):
        """
        Recursive scan to build the types, tables, and table relations.
        """
        def handle_list_or_dict(stack):
            (child_name, obj, parent_name, pobj) = self.find_parent_child_tables(stack)
            if child_name == self.root:
                return
            if child_name not in self.tables:
                self.tables[child_name] = Table(name=child_name, parent=parent_name, exclusions=exclusions)
        def handle_item(stack):
            (item_name, obj) = stack[-1]
            if item_name not in self.sql_types:
                sqltype = SqlTypeEnum.type_of(obj)
                self.sql_types[item_name] = sqltype
            else:
                # item_name was already in sql_types, but here we need to allow some flexibilty.
                # bad formatted xml with blanks or nulls or missing data will leave the item type as NULL
                # when we know from later data that the type is actually more specific.
                # so here we allow NULL to be upgraded to something else.
                # allow NULL -> REAL, INTEGER, TEXT, BLOB
                # allow BLOB -> REAl, INTEGER, TEXT
                existing_type = self.sql_types[item_name]
                new_type = SqlTypeEnum.type_of(obj)
                if existing_type == SqlTypeEnum.NULL and new_type in XML2SQLTransormer.NULL_MIGRATION:
                    self.sql_types[item_name] = new_type
                elif existing_type == SqlTypeEnum.BLOB and new_type in XML2SQLTransormer.BLOB_MIGRATION:
                    self.sql_types[item_name] = new_type
            # add item to the table columns
            (child_name, obj, parent_name, pobj) = self.find_parent_child_tables(stack)
            t = self.tables[child_name]
            t.columns.add(item_name)

        Walker(self.logger, item_handler_func=handle_item, list_handler_func=handle_list_or_dict, dict_handler_func=handle_list_or_dict).walk(self.root, self.json)

        # post scan add 'id' to empty tables
        for k,v in self.tables.items():
            if len(v.columns) == 0:
                v.columns.add('id')
        # post scan to convert NULL sql_types to TEXT
        for k,v in self.sql_types.items():
            if v == SqlTypeEnum.NULL:
                self.sql_types[k] = SqlTypeEnum.TEXT
        for k,v in self.sql_types.items():
            log.debug(self.logger, {
                "name"      : __name__,
                "method"    : "scan_all",
                "column"    : str(k),
                "type"      : sql_type_str(v)
                })
        for k,v in self.tables.items():
            log.debug(self.logger, {
                "name"      : __name__,
                "method"    : "scan_all",
                "table"     : str(k),
                "def"       : str(v)
                })
        return self

    def find_table(self, stack):
        x = 1
        while True:
            if x > len(stack):
                return (None, None, None)
            (name, curobj) = stack[-x]
            if (SqlTypeEnum.type_of(name) == SqlTypeEnum.TEXT) and (isinstance(curobj, dict) or isinstance(curobj, list) and name != self.root):
                return (name, curobj, -x)
            x = x + 1

    def find_parent_child_tables(self, stack):
        (child, obj, cidx) = self.find_table(stack)
        (parent, pobj, pidx) = self.find_table(stack[:cidx].copy())
        return (child, obj, parent, pobj)

    def table2ddl(self, t):
        local_column_tuples         = [(self.sqlite_sanitize(c), sql_type_str(self.sql_types[c])) for c in t.columns]
        parent_column_pname_type    = []
        foreign_key_str_lst         = []
        if t.parent != None and t.parent in self.tables:
            parent = self.tables[t.parent]
            parent_column_pname_type = ["%s_%s %s"%(self.sqlite_sanitize(parent.name), a,b) for (a,b) in [(self.sqlite_sanitize(c), sql_type_str(self.sql_types[c])) for c in parent.primary_key()]]
            parent_primary_key_columns = ["%s_%s" % (parent.name, pk) for pk in parent.primary_key()]
            foreign_key_str_lst.append("FOREIGN KEY ({foreign_keys}) REFERENCES {parent_table}({parent_pk_cols})".format(
                parent_table=self.sqlite_sanitize(parent.name),
                foreign_keys=", ".join(self.sqlite_sanitize_all(parent_primary_key_columns)),
                parent_pk_cols=", ".join(self.sqlite_sanitize_all(parent.primary_key()))))
        all_columns = []
        all_columns.extend(local_column_tuples)
        all_columns_lst = ["%s %s"%(a,b) for (a,b) in all_columns]
        all_columns_lst.extend(parent_column_pname_type)
        all_columns_lst.extend(foreign_key_str_lst)
        return "CREATE TABLE IF NOT EXISTS {name} ({columns}, PRIMARY KEY ({primary_key}));".format(
                name=self.sqlite_sanitize(t.name), 
                columns=", ".join(all_columns_lst),
                primary_key=", ".join(self.sqlite_sanitize_all(t.primary_key())))

    def ddl(self):
        for name, table in self.tables.items():
            yield self.table2ddl(table)
    
    def query_sql(self):
        return []


    def insertion_sql(self):
        assert self.json is not None
        retval = []

        def sql(name, columns, values):
            sql_txt = """INSERT OR IGNORE INTO {table} ({columns}) VALUES ({values});""".format(table=name, columns=", ".join(columns), values=", ".join(values))
            return sql_txt
       
        def get_kv(keys:list, obj:dict):
            assert isinstance(obj, dict)
            sortedkeys = sorted(keys)
            for sk in sortedkeys:
                if sk not in obj:
                    raise Exception("ERROR: sk: '%s' not in obj keys: '%s'" % (sk, obj.keys()))
            values = [obj[k] for k in sortedkeys]
            return (sortedkeys, values)

        def handle_list(stack):
            (name, obj) = stack[-1]
            t = self.tables[name]
            if t.columns == {'id'}:
                synthetic_id = str(uuid.uuid4())
                t.last_id = synthetic_id

        def handle_dict(stack):
            (realname, obj) = stack[-1]
            if realname == self.root:
                return
            (name, _, parent_name, pobj) = self.find_parent_child_tables(stack)

            t = self.tables[name]

            local_keys          = []
            local_values        = []
            parent_pk_keys      = []
            parent_pk_values    = []

            if t.primary_key() == ['id'] and 'id' not in obj.keys():
                synthetic_id = str(uuid.uuid4())
                obj['id'] = synthetic_id
                t.last_id = synthetic_id

            if t.parent is not None and t.parent != self.root and t.parent == parent_name and parent_name in self.tables:
                parent_t = self.tables[parent_name]
                parent_pk = parent_t.primary_key()
                if isinstance(pobj, list):
                    assert parent_pk == ['id']
                    assert parent_t.last_id is not None
                    parent_pk_keys = parent_pk
                    parent_pk_values = [parent_t.last_id]
                else:
                    (parent_pk_keys, parent_pk_values) = get_kv(parent_pk, pobj)

            (local_keys, local_values) = get_kv(t.columns, obj)

            columns = []
            columns.extend(self.sqlite_sanitize_all(local_keys))
            columns.extend(["%s_%s"%(self.sqlite_sanitize(t.parent), self.sqlite_sanitize(k)) for k in parent_pk_keys])
    
            values  = []
            values.extend(self.sqlite_sanitize_values(local_keys, local_values))
            values.extend(self.sqlite_sanitize_values(parent_pk_keys, parent_pk_values))
            
            retval.append(sql(self.sqlite_sanitize(name), columns, values))

        #Walker(self.logger, dict_handler_func=handle_dict, list_handler_func=handle_list).walk(self.root, self.json)
        Walker(self.logger, dict_handler_func=handle_dict).walk(self.root, self.json)
        return retval


    def quote_identifier(self, s, errors="strict"):
        """
        https://stackoverflow.com/questions/6514274/how-do-you-escape-strings-for-sqlite-table-column-names-in-python
        """
        encodable = s.encode("utf-8", errors).decode("utf-8")

        nul_index = encodable.find("\x00")

        if nul_index >= 0:
            error = UnicodeEncodeError("NUL-terminated utf-8", encodable,
                                       nul_index, nul_index + 1, "NUL not allowed")
            error_handler = codecs.lookup_error(errors)
            replacement, _ = error_handler(error)
            encodable = encodable.replace("\x00", replacement)

        return "\"" + encodable.replace("\"", "\"\"") + "\""

    def sqlite_sanitize_values(self, columns, values):
        def _sani_by_type(t, v):
            if v == None:
                s_values.append("\"\"")
            else:
                if t == SqlTypeEnum.NULL:
                    # should not have NULL types (these are converted to TEXT type
                    # at the end of the initial scan)
                    s_values.append("\"\"")
                elif t == SqlTypeEnum.TEXT:
                    s_values.append(self.quote_identifier(v))
                elif t == SqlTypeEnum.INTEGER:
                    s_values.append(v)
                elif t == SqlTypeEnum.REAL:
                    s_values.append(v)
                elif t == SqlTypeEnum.BLOB:
                    s_values.append(base64.b64encode(v))
                else:
                    log.error(self.logger, {
                        "name"      : __name__,
                        "method"    : "sqlite_sanitize_values._sani_by_type",
                        "column"    : str(c),
                        "value"     : str(v),
                        "type"      : str(t),
                        "sql_types" : str(self.sql_types),
                        "ERROR"     : "Failed to sanitize value",
                        })

        s_values = []
        for (c,v) in zip(columns, values):
            if c in self.sql_types:
                _sani_by_type(self.sql_types[c], v)
            else:
                # could not find the type, which is weird b/c all the column types
                # shoud be accounted for. so we can assume this is a strange
                # error case...
                log.warning(self.logger, {
                    "name"      : __name__,
                    "method"    : "sqlite_sanitize_values",
                    "column"    : str(c),
                    "value"     : str(v),
                    "sql_types" : str(self.sql_types),
                    })
                _sani_by_type(SqlTypeEnum.type_of(v), v)

        return s_values

    def sqlite_sanitize_all(self, l):
        """
        Sanitize table names and field names prior for sqlite
        """
        return [self.sqlite_sanitize(x) for x in l]

    def sqlite_sanitize(self, s):
        """
        Sanitize table names and field names prior for sqlite
        """
        return re.sub('[^a-zA-Z0-9_]', '', s).lower()

    def _find_parent_table(self, table_name, table_relations, table_columns):
        """
        There's been some wonkiness in the path, where 'report_item' was stored multiple times in the
        path, resulting in an infinite loop.
        """
        if table_name == self.root:
            return None
        path = table_relations[table_name]
        parent = path.pop()
        while parent == table_name:
            parent = path.pop()
        if parent == self.root:
            return None
        return parent

def parse(logger, resource_name, input_files, input_dir, output_dir, pk_exclusions):
    for f in input_files:
        yield parse_file(logger, resource_name, f, input_dir, output_dir, pk_exclusions)

def parse_file(logger, resource_name, xml_input_file_name, input_dir, output_dir, pk_exclusions):
    chlogger = logger.getChild(__name__)
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        (base, ext) = os.path.splitext(xml_input_file_name)
        outfile = os.path.join(output_dir, "%s.sql" % (base))
        infile  = os.path.join(input_dir, xml_input_file_name)
        total_ddl = 0
        total_sql = 0
        with open(outfile, 'w') as outfh:
            with open(infile, 'r') as infh:
                # all the work happens here
                xst = XML2SQLTransormer(chlogger, infh).parse().scan_all(pk_exclusions)
                # check that the ddl and sql is correct
                # if this fails then it means the ddl/sql combination is incorrect
                sqllst = []
                for ddl in xst.ddl():
                    sqllst.append(ddl)
                for sql in xst.insertion_sql():
                    sqllst.append(sql)
                sqltext = "\n".join(sqllst)
                #db = sqlite3.connect("file::memory:?cache=shared")
                db = sqlite3.connect(":memory:")
                db.executescript(sqltext)
                # all good
                outfh.write(sqltext)
        log.info(chlogger, {
            "src":resource_name, 
            "action":"parse_file",
            "infile": infile,
            "outfile":outfile,
            })
        return xml_input_file_name
    except Exception as e:
        tb = traceback.format_exc()
        log.error(chlogger, {
            "src":resource_name, 
            "action":"parse",
            "xml_file":xml_input_file_name,
            "msg":"parse failed",
            "ERROR" : "Failed to parse xml file",
            "exception": str(e),
            "trace":str(tb),
            })

if __name__ == "__main__":
    infile = sys.argv[1]
    if len(sys.argv) > 2:
        loglevel = sys.argv[2]
    else:
        loglevel = "INFO"
    log.configure_logging()
    logger = logging.getLogger(__name__)
    logger.setLevel(loglevel)
    log.debug(logger, {
        "name"      : __name__,
        "method"    : "main",
        "src"       : "xmlparser.py"
        })
    with open(infile, 'r') as f:
        xst = XML2SQLTransormer(logger, f).parse().scan_all(['value'])
        for d in xst.ddl():
            print(d)
        for sql in xst.insertion_sql():
            print(sql)
#        for query_sql in xst.query_sql():
#            print(query_sql)
