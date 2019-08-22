def new_xml_files(resource_name, state_file, xml_dir):
    """Return a list of xml files that are not present in the state file""" 
    return new_files(resource_name, state_file, zip_dir, "xml")

def parse(resource_name, xml_files, xml_dir, xml_parser_func):
    """Generator to parse each xml_files and return an 'entry' for each"""
    for f in xml_files:
        t = os.path.join(xml_dir, f)
        try:
            yield (f, file_parser_func(t))
        except Exception as e:
            logging.error({
                "src":resource_name, 
                "action":"parse_file",
                "error":e,
                "file":f,
                "msg":"failed to parse file"
                })

def date_8601(date):
    return "%s 00:00:00.000" % date

def posix(timestamp):
    try:
        return dt.datetime.fromisoformat(timestamp).timestamp()
    except Exception as e:
        logging.error({
            "src":RESOURCE_NAME, 
            "action":"posix",
            "timestamp":timestamp,
            "error":e,
            "msg":"failed to parse timestamp"
            })
        return -1

class XmlKeyException(Exception):
    pass

def value(name, dom):
    try:
        return value_of(get_element(name, dom))
    except Exception as e:
        raise XmlKeyException("{'key':%s, 'error':%s}"%(name, e))

def get_element(name, dom):
    try:
        return get_elements(name, dom)[0]
    except Exception as e:
        raise XmlKeyException("{'key':%s, 'error':%s}"%(name, e))

def get_elements(name, dom):
    try:
        e = dom.getElementsByTagNameNS(NS, name)
    except:
        e = dom.getElementsByTagName(name)
    return e

def value_of(node):
    return node.firstChild.nodeValue

