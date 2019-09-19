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

from edl.resources.exec import runyield
from jinja2 import Environment, PackageLoader, select_autoescape
from pathlib import Path
from shutil import make_archive, rmtree
import edl.resources.log as log
import os
import shutil
import stat
import sys
import tarfile
import re
import requests
import json
import traceback

STAGES  = ['download', 'unzip', 'parse', 'insert', 'save']
DIRS    = ['zip', 'xml', 'sql', 'db']
PROCS   = ['10_down.py', '20_unzp.py', '30_pars.py', '40_inse.py', '50_save.py']
STAGE_DIRS = dict(zip(STAGES, DIRS))
STAGE_PROCS = dict(zip(STAGES, PROCS))


def create(logger, ed_path, feed, maintainer, company, email, url, start_date, delay):
    """
    start_date : list of numbers : [2019,09,1]
    """
    chlogger = logger.getChild(__name__)
    new_feed_dir = os.path.join(ed_path, 'data', feed)
    try:
        os.mkdir(new_feed_dir)
        log.debug(chlogger, {
            "name"      : __name__,
            "method"    : "create",
            "path"      : ed_path,
            "feed"      : feed,
            "dir"       : new_feed_dir,
            "message"   : "created directory"
            })
        template_files = [
                "LICENSE","Makefile","README.md",
                "src/10_down.py","src/20_unzp.py","src/30_pars.py",
                "src/40_inse.py", "src/50_save.py", "manifest.json"
                ]
        env = Environment(
            loader=PackageLoader('edl', 'templates'),
            autoescape=select_autoescape(['py'])
        )
        m = {
                'NAME'      : feed,
                'MAINTAINER': maintainer,
                'COMPANY'   : company,
                'EMAIL'     : email,
                'DATA_URL'  : url,
                'REPO_URL'  : "https://github.com/energy-analytics-project/%s" % feed,
                'START'     : start_date,
                'DELAY'     : delay
        }
        for tf in template_files:
            template    = env.get_template(tf)
            target      = os.path.join(new_feed_dir, tf)
            path        = os.path.dirname(target)
            if not os.path.exists(path):
                os.makedirs(path)
            with open(target, 'w') as f:
                f.write(template.render(m))
                log.debug(chlogger, {
                    "name"      : __name__,
                    "method"    : "create",
                    "path"      : ed_path,
                    "feed"      : feed,
                    "target"    : target,
                    "message"   : "rendered target"
                    })

        hidden_files = ['gitignore', 'gitattributes']
        for hf in hidden_files:
            template    = env.get_template(hf)
            target      = os.path.join(new_feed_dir, ".%s" % hf)
            with open(target, 'w') as f:
                f.write(template.render(m))
                log.debug(chlogger, {
                    "name"      : __name__,
                    "method"    : "create",
                    "path"      : ed_path,
                    "feed"      : feed,
                    "target"    : target,
                    "message"   : "rendered target"
                    })
        for src_file in os.listdir(os.path.join(new_feed_dir, 'src')):
            fp = os.path.join(new_feed_dir, 'src', src_file)
            f = Path(fp)
            f.chmod(f.stat().st_mode | stat.S_IEXEC)
            log.debug(chlogger, {
                "name"      : __name__,
                "method"    : "create",
                "path"      : ed_path,
                "feed"      : feed,
                "file"      : fp,
                "message"   : "chmod +x"
                })
        
        for d in DIRS:
            os.makedirs(os.path.join(new_feed_dir, d))
        return feed
    except Exception as e:
        tb = traceback.format_exc()
        log.critical(chlogger, {
            "name"      : __name__,
            "method"    : "create",
            "path"      : ed_path,
            "feed"      : feed,
            "ERROR"     : "FAILED to create feed",
            "exception" : str(e),
            "trace":str(tb),
            })
        # return nothing

def invoke(logger, feed, ed_path, command):
    chlogger = logger.getChild(__name__)
    target_dir = os.path.join(ed_path, 'data', feed)
    log.debug(chlogger, {
                "name"      : __name__,
                "method"    : "invoke",
                "path"      : ed_path,
                "feed"      : feed,
                "command"   : command
        })
    if not os.path.exists(target_dir):
        log.critical(chlogger, {
                    "name"      : __name__,
                    "method"    : "invoke",
                    "path"      : ed_path,
                    "feed"      : feed,
                    "command"   : command,
                    "target_dir": target_dir,
                    "ERROR"     : "target_dir does not exist"
            })
    else:
        return runyield([command], target_dir)

def status(logger, feed, ed_path, separator, header):
    chlogger = logger.getChild(__name__)
    target_dir = os.path.join(ed_path, 'data', feed)
    if not os.path.exists(target_dir):
        log.critical(chlogger, {
                    "name"      : __name__,
                    "method"    : "status",
                    "path"      : ed_path,
                    "feed"      : feed,
                    "separator" : separator,
                    "header"    : header,
                    "target_dir": target_dir,
                    "ERROR"     : "target_dir does not exist"
            })
    if header:
        yield separator.join(["feed name","downloaded","unzipped","parsed", "inserted"])
    txtfiles = ["zip/state.txt", "xml/state.txt", "sql/state.txt", "db/state.txt"]
    counts = [str(lines_in_file(os.path.join(target_dir, f))) for f in txtfiles]
    status = [feed]
    status.extend(counts)
    yield separator.join(status)


def pre_reset(logger, feed, ed_path, stage):
    return os.path.join(ed_path, 'data', feed, STAGE_DIRS[stage])

def reset(logger, feed, ed_path, stage):
    chlogger = logger.getChild(__name__)
    p = pre_reset(logger, feed, ed_path, stage)
    try:
        if os.path.exists(p):
            shutil.rmtree(p)
            log.debug(chlogger, {
                "name"      : __name__,
                "method"    : "reset",
                "path"      : ed_path,
                "feed"      : feed,
                "target_dir": p,
                "message"   : "removed target_dir",
                })
    except Exception as e:
        log.critical(chlogger, {
            "name"      : __name__,
            "method"    : "reset",
            "path"      : ed_path,
            "feed"      : feed,
            "target_dir": p,
            "ERROR"     : "failed to remove target_dir",
            "exception" : str(e)
            })
    try:
        if not os.path.exists(p):
            os.makedirs(p)
            log.debug(chlogger, {
                "name"      : __name__,
                "method"    : "reset",
                "path"      : ed_path,
                "feed"      : feed,
                "target_dir": p,
                "message"   : "makedirs target_dir",
                })
    except Exception as e:
        log.critical(chlogger, {
            "name"      : __name__,
            "method"    : "reset",
            "path"      : ed_path,
            "feed"      : feed,
            "target_dir": p,
            "ERROR"     : "failed to makedirs target_dir",
            "exception" : str(e)
            })
    return p

def lines_in_file(f):
    try:
        with open(f, 'r') as x:
            lines = x.readlines()
            return len(lines)
    except:
        return 0

def src_files(logger, feed, ed_path):
    chlogger = logger.getChild(__name__)
    feed_dir    = os.path.join(ed_path, 'data', feed)
    src_dir     = os.path.join(feed_dir, 'src')
    src_files   = sorted(os.listdir(src_dir))
    log.debug(chlogger, {
            "name"      : __name__,
            "method"    : "src_files",
            "path"      : ed_path,
            "feed"      : feed,
            "feed_dir"  : feed_dir,
            "src_dir"   : src_dir,
            "src_files" : src_files
        })
    return src_files

def process_all_stages(logger, feed, ed_path):
    chlogger = logger.getChild(__name__)
    found_src_files = src_files(logger, feed, ed_path)
    if len(found_src_files) < 1:
        log.critical(chlogger, {
                "name"      : __name__,
                "method"    : "process_all_stages",
                "path"      : ed_path,
                "feed"      : feed,
                "src_files" : found_src_files,
                "ERROR"     : "No files found, nothing to process"
            })
        return
    log.debug(chlogger, {
            "name"      : __name__,
            "method"    : "process_all_stages",
            "path"      : ed_path,
            "feed"      : feed,
            "src_files" : found_src_files
        })
    for src_file in found_src_files:
        yield process_file(logger, feed, ed_path, src_file)

def process_stages(logger, feed, ed_path, stages):
    chlogger = logger.getChild(__name__)
    stage_files = sorted([STAGE_PROCS[s] for s in stages])
    for sf in stage_files:
        if sf in src_files(logger, feed, ed_path):
            yield process_file(logger, feed, ed_path, sf)
        else:
            log.debug(chlogger, {
                    "name"      : __name__,
                    "method"    : "process_stages",
                    "path"      : ed_path,
                    "feed"      : feed,
                    "stage_file": sf,
                    "src_files" : src_files,
                    "ERROR"     : "stage_file not in src_files"
                })

def process_file(logger, feed, ed_path, src_file):
    chlogger    = logger.getChild(__name__)
    feed_dir    = os.path.join(ed_path, 'data', feed)
    rel_path    = os.path.join("src", src_file)
    cmd         = "%s %s" % (rel_path,  log.LOGGING_LEVEL_STRINGS[chlogger.getEffectiveLevel()])

    log.debug(chlogger, {
            "name"      : __name__,
            "method"    : "process_file",
            "path"      : ed_path,
            "feed"      : feed,
            "cmd"       : cmd
        })
    return runyield(cmd, feed_dir)


def archive_locally(logger, feed, ed_path, archivedir):
    chlogger = logger.getChild(__name__)
    try:
        archivedir1 = os.path.expanduser(archivedir)
        if archivedir1.startswith("/"):
            archivedire2 = archivedir1
        else:
            archivedir2 = os.path.join(ed_path, archivedir1)

        archive_name = os.path.join(archivedir2, feed)
        root_dir = os.path.expanduser(os.path.join(ed_path, 'data', feed))
        log.debug(chlogger, {
                "name"      : __name__,
                "method"    : "archive_locally",
                "path"      : ed_path,
                "feed"      : feed,
                "target_dir": archivedir2,
                "archive_name": archive_name,
                "root_dir"  : root_dir
            })
        return make_archive(archive_name, 'gztar', root_dir)
    except Exception as e:
        log.critical(chlogger, {
                "name"      : __name__,
                "method"    : "archive_locally",
                "path"      : ed_path,
                "feed"      : feed,
                "target_dir": archivedir2,
                "archive_name": archive_name,
                "root_dir"  : root_dir,
                "ERROR"     : "make archive failed",
                "exception" : str(e)
            })

def restore_locally(logger, feed, ed_path, archive):
    chlogger = logger.getChild(__name__)
    tf = tarfile.open(archive)
    feed_dir = os.path.join(ed_path, 'data', feed)
    if os.path.exists(feed_dir):
        log.critical(chlogger, {
                "name"      : __name__,
                "method"    : "restore_locally",
                "path"      : ed_path,
                "feed"      : feed,
                "archive"   : archive,
                "feed_dir"  : feed_dir,
                "ERROR"     : "Must delete the feed_dir before restoring."
                })
    else:
        try:
            tf.extractall(os.path.join(ed_path, 'data', feed))
            return feed_dir
        except Exception as e:
            log.critical(chlogger, {
                    "name"      : __name__,
                    "method"    : "restore_locally",
                    "path"      : ed_path,
                    "feed"      : feed,
                    "archive"   : archive,
                    "feed_dir"  : feed_dir,
                    "ERROR"     : "Failed to restore archive to feed_dir",
                    "exception" : str(e)
                    })

def archive_to_s3(logger, feed, ed_path, service, operation):
    """
    Archive feed to an S3 bucket.
    """
    chlogger    = logger.getChild(__name__)
    feed_dir    = os.path.join(ed_path, 'data', feed)
    s3_dir      = os.path.join('eap', 'energy-dashboard', 'data', feed)
    if operation == "copy":
        cmd         = "rclone copy --verbose --include=\"*.zip\" --include=\"*.sql\" --include=\"*.db\" --include=\"*.txt\" %s %s:%s" % (feed_dir, service, s3_dir)
    elif operation == "sync":
        cmd         = "rclone sync --verbose %s %s:%s" % (feed_dir, service, s3_dir)
    else:
        log.critical(chlogger, {
            "name"      : __name__,
            "method"    : "archive_to_s3",
            "path"      : ed_path,
            "op"        : operation,
            "feed"      : feed,
            "s3_dir"    : s3_dir,
            "cmd"       : cmd,
            "ERROR"     : "unknown operation"
            })
    log.info(chlogger, {
            "name"      : __name__,
            "method"    : "archive_to_s3",
            "path"      : ed_path,
            "op"        : operation,
            "feed"      : feed,
            "s3_dir"    : s3_dir,
            "cmd"       : cmd,
        })
    return runyield([cmd], feed_dir)

def restore_from_s3(logger, feed, ed_path, service):
    """
    Restore feed from an S3 bucket.

    It'd be easy if we could simply 'rclone' from the S3 service and
    have the entire bucket replicated here. I've not had any luck with
    that approach.

    Here's the brute force solution. Use the download state file,
    'xml/state.txt', to direct the download operations.  
    """
    chlogger    = logger.getChild(__name__)

    endpoints = {
            'digitalocean'  : 'sfo2.digitaloceanspaces.com',
            'wasabi'        : 's3.us-west-1.wasabisys.com'
    }

    feed_dir    = os.path.join(ed_path, 'data', feed)
    outdir      = os.path.join(feed_dir, 'zip')
    s3_dir      = os.path.join('eap', 'energy-dashboard', 'data', feed)
    try:
        with open(os.path.join(feed_dir, 'xml', 'state.txt'), 'r') as zipfiles:
            for zf in zipfiles:
                zf = zf.rstrip()
                s3_file = "%s/zip/%s" % (s3_dir, zf)
                url = "https://%s/%s/zip/%s" % (endpoints[service], s3_dir, zf)
                r = requests.get(url)
                if r.status_code == 200:
                    with open(os.path.join(outdir, zf), 'wb') as fd:
                        for chunk in r.iter_content(chunk_size=128):
                            fd.write(chunk)
                    logger.info(chlogger, {
                        "name"      : __name__,
                        "method"    : "restore_from_s3",
                        "feed"      : feed,
                        "path"      : ed_path,
                        "feed_dir"  : feed_dir,
                        "outdir"    : outdir,
                        "s3_file"   : s3_file,
                        "service"   : service,
                        "url"       : url
                        })
                else:
                    log.error(chlogger, {
                        "name"      : __name__,
                        "method"    : "restore_from_s3",
                        "feed"      : feed,
                        "path"      : ed_path,
                        "feed_dir"  : feed_dir,
                        "outdir"    : outdir,
                        "s3_file"   : s3_file,
                        "service"   : service,
                        "url"       : url,
                        "ERROR"     : "Failed to retrieve artifact from S3",
                        })

                # return downloaded urls
                yield url
    except Exception as e:
        log.critical(chlogger, {
                "name"      : __name__,
                "method"    : "restore_from_s3",
                "feed"      : feed,
                "path"      : ed_path,
                "feed_dir"  : feed_dir,
                "outdir"    : outdir,
                "s3_dir"    : s3_dir,
                "service"   : service,
                "ERROR"     : "Failed to restore from S3",
                "exception" : str(e)
                })

def scanner(logger, feed, ed_path, xmlfile):
    chlogger    = logger.getChild(__name__)
    try:
        feed_dir    = os.path.join(ed_path(), 'data', feed)
        xml_dir     = os.path.join(feed_dir, 'xml')
        manifest    = os.path.join(feed_dir, 'manifest.json')
        with open(manifest, 'r') as f:
            obj = json.loads(f.read())
        pk_exc = obj.pop('pk_exclusion', ['value'])
        if not xmlfile.startswith(xml_dir):
            xmlfile = os.path.join(xml_dir, xmlfile)
        logger.debug(chlogger, {
            "name"      : __name__,
            "method"    : "scanner",
            "feed"      : feed,
            "path"      : ed_path,
            "feed_dir"  : feed_dir,
            "xml_dir"   : xml_dir,
            "xml_file"  : xml_file,
            "pk_exc"    : pk_exc
            })
        with open(xmlfile, 'r') as f:
            return xmlparser.XML2SQLTransormer(f).parse().scan_types().scan_tables(pk_exc)
    except Exception as e:
        log.critical(chlogger, {
            "name"      : __name__,
            "method"    : "scanner",
            "feed"      : feed,
            "path"      : ed_path,
            "feed_dir"  : feed_dir,
            "xml_dir"   : xml_dir,
            "xml_file"  : xml_file,
            "pk_exc"    : pk_exc,
            "ERROR"     : "Failed to parse and scan xml_file",
            "exception" : str(e)
            })

def generate_table_creation_ddl(logger, feed, ed_path, xmlfile):
    """
    Generate table creation SQL DDL from xmlfile.
    """
    return list(scanner(logger, feed, ed_path, xmlfile).ddl())

def generate_insertion_sql(logger, feed, ed_path, xmlfile, save):
    """
    Generate insertion SQL from xmlfile.
    """
    return list(scanner(logger, feed, ed_path, xmlfile).insertion_sql())

def get_latest_xml_file(logger, feed, ed_path, xml_dir):
    """
    Return the latest xml file (sorted by name, which includes a timestamp).
    May not actually be the latest due to vagaries in the names.

    If this is ever a problem then stat the files and use the lmod times.
    """
    xml_files   = sorted(list(fs.glob_dir(xml_dir, ".xml")))
    if len(xml_files) < 1:
        log.critical(logger, {
            "name"      : __name__,
            "method"    : "get_latest_xml_file",
            "feed"      : feed,
            "path"      : ed_path,
            "xml_dir"   : xml_dir,
            "ERROR"     : "Failed to parse xml_file",
            })
        return
    return xml_files[-1]

def create_and_save_ddl(logger, feed, ed_path, xmlfile, save):
    """
    Create the SQL DDL from an input xmlfile.
    Save the DDL to the manifest if save==True.
    Grab the latest xml file in the feed if xmlfile is None.
    """
    chlogger    = logger.getChild(__name__)
    feed_dir    = os.path.join(ed_path, 'data', feed)
    xml_dir     = os.path.join(feed_dir, 'xml')
    manifest    = os.path.join(feed_dir, 'manifest.json')

    if xmlfile is None:
        xmlfile = clifeed.get_latest_xml_file(logger, feed, ed_path, xml_dir)
    if not xmlfile.startswith(xml_dir):
        xmlfile = os.path.join(xml_dir, xmlfile)
    ddl = clifeed.generate_table_creation_ddl(logger, feed, path, xmlfile)
    if save:
        obj['ddl_create'] = ddl
        with open(manifest, 'w') as f:
            f.write(json.dumps(obj, indent=4, sort_keys=True))
    return ddl

def manifest_update(logger, feed, ed_path, field, value_str, value_int):
    """
    Update the manifest.field with the value provided.
    """
    feed_dir    = os.path.join(ed_path, 'data', feed)
    manifest    = os.path.join(feed_dir, 'manifest.json')

    value = value_str or value_int

    with open(manifest, 'r') as f:
        obj = json.loads(f.read())
    if value == None or value == "":
        obj.pop(field, None)
    else:
        obj[field] = value
    with open(manifest, 'w') as f:
        f.write(json.dumps(obj, indent=4, sort_keys=True))
