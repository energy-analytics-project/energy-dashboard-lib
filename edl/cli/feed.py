from edl.cli.config import Config
from edl.resources.dbg import debugout
import os

def create(ctx, name, maintainer, company, email, url, start_date_tuple):
    cfg = Config.from_ctx(ctx)
    new_feed_dir = os.path.join(cfg.ed_path(), 'data', name)
    os.mkdir(new_feed_dir)
    if cfg.debug(): debugout("Created directory: %s" % new_feed_dir)
    template_files = ["LICENSE","Makefile","README.md","src/10_down.py","src/20_unzp.py","src/30_inse.py","src/40_save.sh","manifest.json"]
    env = Environment(
        loader=PackageLoader('edc', 'templates'),
        autoescape=select_autoescape(['py'])
    )
    m = {
            'NAME'      : name,
            'MAINTAINER': maintainer,
            'COMPANY'   : company,
            'EMAIL'     : email,
            'DATA_URL'  : url,
            'REPO_URL'  : "https://github.com/energy-analytics-project/%s" % name,
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
    return name

def invoke(ctx, feed, command):
    cfg = Config.from_ctx(ctx)
    target_dir = os.path.join(cfg.ed_path(), 'data', feed)
    if not os.path.exists(target_dir):
        raise Exception("Feed does not exist at: %s" % target_dir)
    echo_exec([command], target_dir)
