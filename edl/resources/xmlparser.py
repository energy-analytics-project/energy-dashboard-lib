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
import xmltodict

class WalkerState():
    def __init__(self, meta=None, dict_handler_func=None, list_handler_func=None, item_handler_func=None, name_handler_func=None):
        def default_dict_handler_func(obj, name, path, state):
            return state
        def default_list_handler_func(obj, name, path, state):
            return state
        def default_item_handler_func(obj, name, path, state):
            return state
        def default_name_handler_func(name):
            return name
        self.meta = meta or {}
        self.dict_handler_func = dict_handler_func or default_dict_handler_func
        self.list_handler_func = list_handler_func or default_list_handler_func
        self.item_handler_func = item_handler_func or default_item_handler_func
        self.name_handler_func = name_handler_func or default_name_handler_func

def walk_object(obj, name, path, state):
    """
    Walk object tree and inkoke handlers based on object type (dict, list, or item).
    """
    name = state.name_handler_func(name)
    if isinstance(obj, dict):
        # obj is dict
        state = state.dict_handler_func(obj, name, path, state)
        for k, v in obj.items():
            state = walk_object(v, k, path + [name], state)
    elif isinstance(obj, list):
        # obj is list
        state = state.list_handler_func(obj, name, path, state)
        for idx, item in enumerate(obj):
            state = walk_object(item, name, path + [name], state)
    else:
        # obj is neither dict or list
        state = state.item_handler_func(obj, name, path, state)
    return state

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
    def __init__(self, xmlfile):
        """
        Following 'Elegant Objects' style, constructor only sets vars. All
        work is delayed until needed.
        """
        self.xmlfile            = xmlfile
        """xmlfile : file obect""" 

        self.json               = None
        """json : json object (typically a dict) resulting from parsing the xmlfile object"""

        self.sql_types          = None
        """sql_types : map of xml element names to sqlite3 value types"""

        self.table_columns      = None
        """table_columns : list of columns tuples of (column name, column type)"""

        self.table_relations    = None
        """table_relations : map of table names to a list of parent tables"""

        self.root               = 'root'
        """root : name of the root node which is really the document root, for which no table is generated"""

    def parse(self):
        """
        Parse the loaded xmlfile and load into the self.json object.
        """
        self.json = xmltodict.parse(self.xmlfile.read())
        # allow method chaining
        return self

    def transform(self):
        """
        Recursive Scan to assemble the databases and database relationships. For
        example, in the XML snippet above we have the following relationships:

            OASISReport -> MessageHeader -> [TimeDate, Source, Version]
            OASISReport -> MessagePayload -> RTO -> REPORT_ITEM -> REPORT_HEADER -> [...]
            OASISReport -> MessagePayload -> RTO -> REPORT_ITEM -> REPORT_DATA -> [...]

        These relationships are represented as FOREIGN KEY references to the 'parent' table.
        This is especially important in the case of the REPORT_HEADER and REPORT_DATA both
        pointing to the empty REPORT_ITEM. If REPORT_ITEM did not exist in the schema, then
        there would be no way to correlate the REPORT_HEADER and the REPORT_DATA.

        Table_columns without any columns have an 'id INTEGER' column injected into them. table_columns with columns
        do not.

        Primary keys are constructed from all of the available columns, sans those provided in the
        'primary_key_exclusions' parameter. Primary keys are used for data integrity. It is important
        that re-running the insertions is idempotent, once inserted, no new records are inserted.

        :param obj:             json object to be inspected, defaults to self.json
        :param obj_name:        name of the object
        :param table_columns:   contains the list of columns for a given table
        :param sql_types:       contains the SqlTypeEnum for each of the found columns from all the table_columns
        :param table_relations: contains the parent-child relationship for each table
        :returns: (table_columns, sql_types, table_relations)

        See: https://stackoverflow.com/questions/38397285/iterate-over-all-items-in-json-object#38397347
        """
        assert self.json is not None

        def handle_dict(obj, name, path, ws):
            table_columns   = ws.meta['table_columns']
            table_relations = ws.meta['table_relations']
            if name not in table_columns:
                table_columns[name] = []
            table_columns[name].extend(set([ws.name_handler_func(k) for k in obj.keys()]) - set(table_columns[name]))
            if name not in table_relations:
                table_relations[name] = path
            return ws

        def handle_item(obj, name, path, ws):
            sql_types       = ws.meta['sql_types']
            if name not in sql_types:
                sql_types[name] = SqlTypeEnum.type_of(obj)
            return ws

        state0 = WalkerState(
                meta={
                    'table_columns':{}, 
                    'sql_types':{'id': SqlTypeEnum.INTEGER}, 
                    'table_relations':{}},
                name_handler_func=self._clean,
                dict_handler_func=handle_dict,
                item_handler_func=handle_item)
        state1 = walk_object(self.json, self.root, [], state0)
        self.table_columns      = state1.meta['table_columns']
        self.sql_types          = state1.meta['sql_types']
        self.table_relations    = state1.meta['table_relations']
        # allow method chaining
        return self

    def creation_ddl(self, primary_key_exclusions=['value']):
        return self._generate_sql_ddl(self.table_columns, self.sql_types, self.table_relations, primary_key_exclusions)

    def insertion_sql(self):
        return []

    def query_sql(self):
        return []

    def _clean(self, s):
        """
        Remove wonky characters from XML attributes, like '@xmlns'.
        """
        return re.sub('[^a-zA-Z0-9_]', '', s).lower()

    def _gen_primary_key(self, table_name, table_columns, sql_types, primary_key_exclusions):
        """
        Return a primary key for the given table. The primary key is composed of all the 
        columns not explicitly excluded, that are also present in the sql_types.
        """
        return [c for c in table_columns[table_name] if c not in primary_key_exclusions and c in sql_types]


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

    def _insert_or_ignore_sql(self, table_name, columns, values):
        return """INSERT OR IGNORE INTO {table} ({columns}) VALUES ({values});""".format(table=table_name, columns=columns, values=values)

    def _generate_sql_ddl(self, xtable_columns, sql_types, table_relations, primary_key_exclusions):
        """
        Generate the SQL DDL to create the tables with the foreign key relationships as described
        by: table_columns sql_types table_relations and primary_key_exclusions, all defined on this class.
        """
        
        def gen_foreign_key_constraints_ddl(parent_table, parent_table_primary_keys):
            return "FOREIGN KEY (%s) REFERENCES %s(%s)" % (
                    ", ".join(["%s_%s" % (parent_table, x) for x in parent_table_primary_keys]), 
                    parent_table, 
                    ", ".join(parent_table_primary_keys))

        # As mentioned earlier, the root node maps to the xml document root, which is does not
        # map to a table that needs to be created. This is just an artifact of the recursive
        # scan that can be removed now. Modify a local copy.
        table_columns = xtable_columns.copy() 
        del table_columns[self.root]

        # Iterate over the tables
        for tbl,cols in table_columns.items():

            # Filter out columns that are not in sql_types. Practically, this is filtering
            # out the forward references in the XML to child elements that will be expressed
            # in the DDL as back references, e.g. foreign keys to a parent table.
            cols = [c for c in cols if c in sql_types] 

            # For those XML elements that have no data, the length of the columns will be zero.
            # However, these table relationships must be maintained, especially since the tables
            # and their relationships are auto-generated. The classic example of this is in the
            # OASIS XML feeds REPORT_ITEM -> {REPORT_HEADER, REPORT_DATA}. There must be a way
            # to maintain the relationship of the header to the data, and that's done via the
            # item table with a synthetic field added below...
            if len(cols) == 0:
                # Table has no columns, so insert a synthetic column.
                # But, there is no need to make this autoincrement, see: https://sqlite.org/autoinc.html.
                cols = cols + ['id']
                table_columns[tbl] = cols

            # Foreign keys etc. represented as lists to make the ",".join() operations nicer later.
            foreign_key_constraints             = []
            fk_column_sql_types                 = []
            column_sql_types                    = []

            # Recursively find a parent table in order to maintain the back reference via the foreign key.
            parent_table = self._find_parent_table(tbl, table_relations, table_columns)

            # The only table that will not have a back reference is the top level table named self.root.
            if parent_table != None:
                # Re-generate the parent table's primary keys. we need this b/c the generation of a
                # foreign key in the child table requires all the parent tables primary keys.
                # See: https://sqlite.org/foreignkeys.html
                parent_table_primary_keys = self._gen_primary_key(parent_table, table_columns, sql_types, primary_key_exclusions)
           
                # Generate the foreign key constrants on 'this' table by passing in the 'parent' table and it's primary keys.
                foreign_key_constraints.append(gen_foreign_key_constraints_ddl(parent_table, parent_table_primary_keys))

                # Express the sql types for the parent table's primary key columns
                for fpk in parent_table_primary_keys:
                    fk_column_sql_types.append("%s_%s %s" % (parent_table, fpk, sql_type_str(sql_types[fpk])))

            # Express the column definitions for 'this' table
            for col in cols:
                if col in sql_types:
                    column_sql_types.append("%s %s" % (col, sql_type_str(sql_types[col])))
                   
            # Express the primary key definition for 'this' table
            primary_key_def = ", ".join(self._gen_primary_key(tbl, table_columns, sql_types, primary_key_exclusions))
            
            # Concatenate column definitions for 'this' table, including all the column definitions needed for the
            # foreign key relations to the 'parent' table.
            column_sql_types.extend(fk_column_sql_types)
            column_sql_types.extend(foreign_key_constraints)
            combined_key_def = ", ".join(column_sql_types)

            # Done
            yield "CREATE TABLE IF NOT EXISTS %s (%s, PRIMARY KEY (%s));" % (tbl, combined_key_def, primary_key_def)


if __name__ == "__main__":
    infile = sys.argv[1]
    with open(infile, 'r') as f:
        xst = XML2SQLTransormer(f).parse().transform()
        for ddl in xst.creation_ddl(['value']):
            print(ddl)
        for insert_sql in xst.insertion_sql():
            print(insert_sql)
        for query_sql in xst.query_sql():
            print(query_sql)
