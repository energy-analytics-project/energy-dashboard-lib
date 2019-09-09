from edl.cli.config import Config
from edl.resources.dbg import debugout
from edl.resources.exec import runyield
import os
import shutil
from jinja2 import Environment, PackageLoader, select_autoescape
from shutil import make_archive, rmtree

STAGES  = ['download', 'unzip', 'parse', 'insert']
DIRS    = ['zip', 'xml', 'sql', 'db']
PROCS   = ['10_down.py', '20_unzp.py', '30_pars.py', '40_inse.py', '50_save.sh']
STAGE_DIRS = dict(zip(STAGES, DIRS))
STAGE_PROCS = dict(zip(STAGES, PROCS))

def create(debug, ed_path, feed, maintainer, company, email, url, start_date_tuple):
    new_feed_dir = os.path.join(ed_path, 'data', feed)
    os.mkdir(new_feed_dir)
    if debug: debugout("Created directory: %s" % new_feed_dir)
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
            if debug: debugout("Rendered '%s'" % target)

    hidden_files = ['gitignore', 'gitattributes']
    for hf in hidden_files:
        template    = env.get_template(hf)
        target      = os.path.join(new_feed_dir, ".%s" % hf)
        with open(target, 'w') as f:
            f.write(template.render(m))
            if debug: debugout("Rendered '%s'" % target)
    return feed

def invoke(debug, feed, ed_path, command):
    target_dir = os.path.join(ed_path, 'data', feed)
    if not os.path.exists(target_dir):
        raise Exception("Feed does not exist at: %s" % target_dir)
    return runyield([command], target_dir)

def status(debug, feed, ed_path, separator, header):
    target_dir = os.path.join(ed_path, 'data', feed)
    if not os.path.exists(target_dir):
        raise Exception("Feed does not exist at: %s" % target_dir)
    if header:
        yield separator.join(["feed name","downloaded","unzipped","parsed", "inserted"])
    txtfiles = ["zip/downloaded.txt", "xml/unzipped.txt", "sql/parsed.txt", "db/inserted.txt"]
    counts = [str(lines_in_file(os.path.join(target_dir, f))) for f in txtfiles]
    status = [feed]
    status.extend(counts)
    yield separator.join(status)


def pre_reset(debug, feed, ed_path, stage):
    p = os.path.join(ed_path, 'data', feed, STAGE_DIRS[stage])
    return p

def reset(p):
    try:
        shutil.rmtree(p)
    except:
        pass
    os.makedirs(p)

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

def restore_locally(ctx, feed, archivedir):
    cfg = Config.from_ctx(ctx)
    if archivedir is None:
        archivedir = os.path.join(cfg.ed_path, 'archive')
    archive_name = os.path.join(archivedir, "%s.tar.gz" % feed)
    tf = tarfile.open(archive_name)
    feed_dir = os.path.join(cfg.ed_path, 'data', feed)
    if os.path.exists(feed_dir):
        return "Must delete the target feed dir '%s' before restoring." % feed_dir
    else:
        return tf.extractall(os.path.join(cfg.ed_path, 'data', feed))


def archive_locally(debug, feed, ed_path, archivedir):
    if archivedir is None:
        archivedir = os.path.join(ed_path, 'archive')
    archive_name = os.path.join(archivedir, feed)
    root_dir = os.path.expanduser(os.path.join(ed_path, 'data', feed))
    return make_archive(archive_name, 'gztar', root_dir)

def archive_to_s3(ctx, feed, service):
    """
    Archive feed to an S3 bucket.
    """
    cfg         = Config.from_ctx(ctx)
    feed_dir    = os.path.join(cfg.ed_path, 'data', feed)
    s3_dir      = os.path.join('eap', 'energy-dashboard', 'data', feed)
    cmd         = "rclone copy --verbose --include=\"zip/*.zip\" --include=\"db/*.db\" %s %s:%s" % (feed_dir, service, s3_dir)
    runyield([cmd], feed_dir)
