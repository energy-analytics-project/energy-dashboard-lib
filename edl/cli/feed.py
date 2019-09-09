from edl.resources.dbg import debugout
from edl.resources.exec import runyield
from jinja2 import Environment, PackageLoader, select_autoescape
from shutil import make_archive, rmtree
import edl.resources.log as log
import os
import shutil
import tarfile

STAGES  = ['download', 'unzip', 'parse', 'insert']
DIRS    = ['zip', 'xml', 'sql', 'db']
PROCS   = ['10_down.py', '20_unzp.py', '30_pars.py', '40_inse.py', '50_save.sh']
STAGE_DIRS = dict(zip(STAGES, DIRS))
STAGE_PROCS = dict(zip(STAGES, PROCS))

def create(logger, ed_path, feed, maintainer, company, email, url, start_date_tuple):
    chlogger = logger.getChild(__name__)
    new_feed_dir = os.path.join(ed_path, 'data', feed)
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
            "src/40_inse.py", "src/50_save.sh", "manifest.json"
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
            'START'     : start_date_tuple,
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
    return feed

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
        log.error(chlogger, {
                    "name"      : __name__,
                    "method"    : "invoke",
                    "path"      : ed_path,
                    "feed"      : feed,
                    "command"   : command,
                    "target_dir": target_dir,
                    "ERROR"     : "target_dir does not exist"
            })
        return []
    else:
        return runyield([command], target_dir)

def status(logger, feed, ed_path, separator, header):
    chlogger = logger.getChild(__name__)
    target_dir = os.path.join(ed_path, 'data', feed)
    if not os.path.exists(target_dir):
        log.error(chlogger, {
                    "name"      : __name__,
                    "method"    : "status",
                    "path"      : ed_path,
                    "feed"      : feed,
                    "separator" : separator,
                    "header"    : header,
                    "target_dir": target_dir,
                    "ERROR"     : "target_dir does not exist"
            })
        return []
    if header:
        yield separator.join(["feed name","downloaded","unzipped","parsed", "inserted"])
    txtfiles = ["zip/downloaded.txt", "xml/unzipped.txt", "sql/parsed.txt", "db/inserted.txt"]
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
        log.error(chlogger, {
            "name"      : __name__,
            "method"    : "reset",
            "path"      : ed_path,
            "feed"      : feed,
            "target_dir": p,
            "ERROR"     : "failed to remove target_dir",
            "exception" : str(e)
            })
    try:
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
        log.error(chlogger, {
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

def process_all_stages(debug, feed, ed_path):
    feed_dir    = os.path.join(ed_path, 'data', feed)
    src_dir     = os.path.join(feed_dir, 'src')
    src_files   = sorted(os.listdir(src_dir))
    for src_file in src_files:
        cmd = os.path.join(src_dir, src_file)
        yield runyield(cmd, feed_dir)

def process_stages(debug, feed, ed_path, stages):
    feed_dir    = os.path.join(ed_path, 'data', feed)
    src_dir     = os.path.join(feed_dir, 'src')
    src_files   = set(os.listdir(src_dir))
    for s in stages:
        if s in src_files:
            cmd = os.path.join(src_dir, s)
            yield runyield(cmd, feed_dir)
        else:
            #logging.error({ })
            pass


def archive_locally(logger, feed, ed_path, archivedir):
    chlogger = logger.getChild(__name__)
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
    try:
        return make_archive(archive_name, 'gztar', root_dir)
    except Exception as e:
        log.debug(chlogger, {
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
        log.error(chlogger, {
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
            log.error(chlogger, {
                    "name"      : __name__,
                    "method"    : "restore_locally",
                    "path"      : ed_path,
                    "feed"      : feed,
                    "archive"   : archive,
                    "feed_dir"  : feed_dir,
                    "ERROR"     : "Failed to restore archive to feed_dir",
                    "exception" : str(e)
                    })

def archive_to_s3(ctx, feed, service):
    """
    Archive feed to an S3 bucket.
    """
    feed_dir    = os.path.join(ed_path, 'data', feed)
    s3_dir      = os.path.join('eap', 'energy-dashboard', 'data', feed)
    cmd         = "rclone copy --verbose --include=\"zip/*.zip\" --include=\"db/*.db\" %s %s:%s" % (feed_dir, service, s3_dir)
    runyield([cmd], feed_dir)
