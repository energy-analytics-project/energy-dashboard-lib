from edl.cli.config import Config
from edl.resources.dbg import debugout
from edl.resources.exec import runyield
import os
import shutil

def create(ctx, maintainer, company, email, url, start_date_tuple):
    feed = ctx.obj['feed']
    cfg = Config.from_ctx(ctx)
    new_feed_dir = os.path.join(cfg.ed_path(), 'data', feed)
    os.mkdir(new_feed_dir)
    if cfg.debug(): debugout("Created directory: %s" % new_feed_dir)
    template_files = ["LICENSE","Makefile","README.md","src/10_down.py","src/20_unzp.py","src/30_inse.py","src/40_save.sh","manifest.json"]
    env = Environment(
        loader=PackageLoader('edc', 'templates'),
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
            if cfg.debug(): debugout("Rendered '%s'" % target)

    hidden_files = ['gitignore', 'gitattributes']
    for hf in hidden_files:
        template    = env.get_template(hf)
        target      = os.path.join(new_feed_dir, ".%s" % hf)
        with open(target, 'w') as f:
            f.write(template.render(m))
            if cfg.debug(): debugout("Rendered '%s'" % target)
    return feed

def invoke(ctx, command):
    feed = ctx.obj['feed']
    cfg = Config.from_ctx(ctx)
    target_dir = os.path.join(cfg.ed_path(), 'data', feed)
    if not os.path.exists(target_dir):
        raise Exception("Feed does not exist at: %s" % target_dir)
    return runyield([command], target_dir)

def status(ctx, separator, header):
    feed = ctx.obj['feed']
    cfg = Config.from_ctx(ctx)
    target_dir = os.path.join(cfg.ed_path(), 'data', feed)
    if not os.path.exists(target_dir):
        raise Exception("Feed does not exist at: %s" % target_dir)
    if header:
        yield separator.join(["feed name","downloaded","unzipped","parsed", "inserted"])
    txtfiles = ["zip/downloaded.txt", "xml/unzipped.txt", "sql/parsed.txt", "db/inserted.txt"]
    counts = [str(lines(os.path.join(target_dir, f))) for f in txtfiles]
    status = [feed]
    status.extend(counts)
    yield separator.join(status)

def reset(ctx, feed, stage):
    stage_dir = {'download' : 'zip', 'unzip' : 'xml', 'parse': 'sql', 'insert':'db'}
    cfg = Config.from_ctx(ctx)
    for s in stage:
        p = os.path.join(cfg.ed_path(), 'data', feed, stage_dir[s])
        if click.confirm('About to delete %s. Do you want to continue?' % p):
            shutil.rmtree(p)
        os.makedirs(p)

def lines(f):
    try:
        with open(f, 'r') as x:
            lines = x.readlines()
            return len(lines)
    except:
        return 0
