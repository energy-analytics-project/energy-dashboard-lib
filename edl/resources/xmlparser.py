#!/usr/bin/env python3

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
import xmltodict

class Walker(object):
    def __init__(self, dict_handler_func=None, list_handler_func=None, item_handler_func=None):
        def default_dict_handler_func(stack):
            pass
        def default_list_handler_func(stack):
            pass
        def default_item_handler_func(stack):
            pass
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
                self._walk(stack + [(name, item)])
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
    def __init__(self, name):
        self.name = name
        """table name"""

        self.local_columns = []
        """columns in this table"""

        self.primary_key = []
        """the primary key (all the local columns minus the exclusions, if any)"""

        self.parent = None
        """name parent of this table, e.g. where this table has foreign key references to"""

        self.children = []
        """
        names of the children of this table:
        * must be declared/inserted into after this table
        * reference this table as their parent
        """

    def __repr__(self):
        return "name: %s, local_columns: %s, primary key: %s, parent: %s, children: %s" % (self.name, self.local_columns, self.primary_key, self.parent, self.children)

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

        self.sql_types          = {'id': SqlTypeEnum.TEXT}
        """sql_types : map : xml element name -> sqlite3 value type"""

        self.tables             = {}
        """tables : map : table name -> Table(), which are just table definitions for sql inserts"""

        self.root               = 'root'
        """root : name of the root node which is really the document root, for which no table is generated"""

    def parse(self, xml_namespace):
        """
        Parse the loaded xmlfile and load into the self.json object.
        """
        self.json = xmltodict.parse(self.xmlfile.read(), process_namespaces=True, namespaces={xml_namespace: None})
        # allow method chaining
        return self

    def scan_types(self):
        """
        Recursive scan to build the types.
        """
        def handle_item(stack):
            (name, obj) = stack[-1]
            if name not in self.sql_types:
                sqltype = SqlTypeEnum.type_of(obj)
                self.sql_types[name] = sqltype
                self.sql_types[self.sqlite_sanitize(name)] = sqltype

        Walker(item_handler_func=handle_item).walk(self.root, self.json)
#        print("#------------------------")
#        for k,v in self.sql_types.items():
#            print("# types: %s -> %s" % (k,v))
#        print("#------------------------")
        return self

    def scan_tables(self, primary_key_exclusions):
        """
        Recursive Scan to assemble the databases and database relationships.
        For example, in the XML snippet above we have the following
        relationships:

            OASISReport -> MessageHeader -> [TimeDate, Source, Version]
            OASISReport -> MessagePayload -> RTO -> REPORT_ITEM -> REPORT_HEADER -> [...]
            OASISReport -> MessagePayload -> RTO -> REPORT_ITEM -> REPORT_DATA -> [...]

        These relationships are represented as FOREIGN KEY references to the
        'parent' table.  This is especially important in the case of the
        REPORT_HEADER and REPORT_DATA both pointing to the empty REPORT_ITEM.
        If REPORT_ITEM did not exist in the schema, then there would be no way
        to correlate the REPORT_HEADER and the REPORT_DATA.

        Table_columns without any columns will have an 'id TEXT' column
        injected into them with uuid.uuid4() values.

        Primary keys are constructed from all of the available columns, sans
        those provided in the 'primary_key_exclusions' parameter. Primary keys
        are used for data integrity. It is important that re-running the
        insertions is idempotent after initial insertion. E.g.  once inserted,
        no new records are inserted.

        See: https://stackoverflow.com/questions/38397285/iterate-over-all-items-in-json-object#38397347
        """
        assert self.json is not None

        def handle_dict_or_list(stack, columns=[]):
            (name, obj) = stack[-1]
            if name == self.root:
                return
            if name not in self.tables:
                self.tables[name] = Table(name=name)
            t = self.tables[name]
            cols = [c for c in set(columns) if c in self.sql_types]
            t.local_columns = list(set(t.local_columns).union(set(cols)))
            # Table has no columns, so insert a synthetic column. 'id' maps to a TEXT field.
            if len(t.local_columns) == 0:
                t.local_columns = t.primary_key = ['id']
                #print("# patch: %s" % t)
            pks = set(t.local_columns) - set(primary_key_exclusions)
            t.primary_key = list(set(t.primary_key).union(pks))
            if len(stack) > 1:
                (parent_name, parent_obj) = stack[-2]
                if parent_name is not None and parent_name is not self.root and parent_name is not name:
                    t.parent = parent_name
                    if t.parent in self.tables:
                        parent = self.tables[t.parent]
                        ch = set(parent.children).add(name)

        def handle_dict(stack):
            (name, obj) = stack[-1]
            handle_dict_or_list(stack, obj.keys())

        def handle_list(stack):
            handle_dict_or_list(stack)

        Walker(dict_handler_func=handle_dict, list_handler_func=handle_list).walk(self.root, self.json)
#        print("#------------------------")
#        for k,v in self.tables.items():
#            print("# tables: %s" % v)
#        print("#------------------------")
        return self
        
    
    def table2ddl(self, t):
        local_column_tuples         = [(self.sqlite_sanitize(c), sql_type_str(self.sql_types[c])) for c in t.local_columns]
        parent_column_pname_type    = []
        foreign_key_str_lst         = []
        if t.parent != None and t.parent in self.tables:
            parent = self.tables[t.parent]
            parent_column_pname_type = ["%s_%s %s"%(self.sqlite_sanitize(parent.name), a,b) for (a,b) in [(self.sqlite_sanitize(c), sql_type_str(self.sql_types[c])) for c in parent.primary_key]]
            parent_primary_key_columns = ["%s_%s" % (parent.name, pk) for pk in parent.primary_key]
            foreign_key_str_lst.append("FOREIGN KEY ({foreign_keys}) REFERENCES {parent_table}({parent_pk_cols})".format(
                parent_table=self.sqlite_sanitize(parent.name),
                foreign_keys=", ".join(self.sqlite_sanitize_all(parent_primary_key_columns)),
                parent_pk_cols=", ".join(self.sqlite_sanitize_all(parent.primary_key))))
        all_columns = []
        all_columns.extend(local_column_tuples)
        all_columns_lst = ["%s %s"%(a,b) for (a,b) in all_columns]
        all_columns_lst.extend(parent_column_pname_type)
        all_columns_lst.extend(foreign_key_str_lst)
        return "CREATE TABLE IF NOT EXISTS {name} ({columns}, PRIMARY KEY ({primary_key}));".format(
                name=self.sqlite_sanitize(t.name), 
                columns=", ".join(all_columns_lst),
                primary_key=", ".join(self.sqlite_sanitize_all(t.primary_key)))

    def ddl(self):
        for name, table in self.tables.items():
            yield self.table2ddl(table)
    
    def query_sql(self):
        return []


    def insertion_sql(self):
        assert self.json is not None
        retval = []

        def sql(name, columns, values):
            return """INSERT OR IGNORE INTO {table} ({columns}) VALUES ({values});""".format(table=name, columns=", ".join(columns), values=", ".join(values))
       
        def get_kv(keys, obj):
            sortedkeys = sorted(keys)
            values = [obj[k] for k in sortedkeys]
            return (sortedkeys, values)

        def handle_dict(stack):
            (name, obj) = stack[-1]

            if name == self.root:
                return

            t = self.tables[name]

            local_keys          = []
            local_values        = []
            parent_pk_keys      = []
            parent_pk_values    = []

            if t.primary_key == ['id'] and 'id' not in obj.keys():
                obj['id'] = str(uuid.uuid4())

            if t.parent is not None and t.parent in self.tables and len(stack) > 1:
                for i in range(len(stack)-1, 0, -1):
                    (pname, pobj) = stack[i]
                    if pname == t.parent:
                        pt = self.tables[t.parent]
                        (parent_pk_keys, parent_pk_values) = get_kv(pt.primary_key, pobj)
                        break

            (local_keys, local_values) = get_kv(t.local_columns, obj)

            columns = []
            columns.extend(self.sqlite_sanitize_all(local_keys))
            columns.extend(["%s_%s"%(self.sqlite_sanitize(t.parent), self.sqlite_sanitize(k)) for k in parent_pk_keys])
    
            values  = []
            values.extend(self.sqlite_sanitize_values(local_keys, local_values))
            values.extend(self.sqlite_sanitize_values(parent_pk_keys, parent_pk_values))
            
            retval.append(sql(name, columns, values))

        Walker(dict_handler_func=handle_dict).walk(self.root, self.json)
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
        s_values = []
        for (c,v) in zip(columns, values):
            if SqlTypeEnum.type_of(v) == SqlTypeEnum.TEXT:
                s_values.append(self.quote_identifier(v))
            else:
                s_values.append(v)
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

def parse(resource_name, input_files, input_dir, output_dir, pk_exclusions, xml_namespace):
    for f in input_files:
        yield parse_file(resource_name, f, input_dir, output_dir, pk_exclusions, xml_namespace)

def parse_file(resource_name, xml_input_file_name, input_dir, output_dir, pk_exclusions, xml_namespace):
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        (base, ext) = os.path.splitext(xml_input_file_name)
        outfile = os.path.join(output_dir, "%s.sql" % (base))
        total_ddl = 0
        total_sql = 0
        with open(outfile, 'w') as outfh:
            with open(os.path.join(input_dir, xml_input_file_name), 'r') as infh:
                xst = XML2SQLTransormer(infh).parse(xml_namespace).scan_types().scan_tables(pk_exclusions)
                for d in xst.ddl():
                    outfh.write("%s\n" % d)
                    total_ddl += 1
                for sql in xst.insertion_sql():
                    outfh.write("%s\n" % sql)
                    total_sql += 1
        logging.info({
            "src":resource_name, 
            "action":"parse_file",
            "infile": xml_input_file_name,
            "outfile":outfile,
            "total_ddl":total_ddl,
            "total_sql":total_sql
            })
        return xml_input_file_name
    except Exception as e:
        logging.error({
            "src":resource_name, 
            "action":"parse",
            "error":e,
            "filename":xml_input_file_name,
            "msg":"parse failed"
            })
        return ""

if __name__ == "__main__":
    infile = sys.argv[1]
    with open(infile, 'r') as f:
        xst = XML2SQLTransormer(f).parse().scan_types().scan_tables(['value'])
        for d in xst.ddl():
            print(d)
        for sql in xst.insertion_sql():
            print(sql)
#        for query_sql in xst.query_sql():
#            print(query_sql)
