import os
import sys
import json
from edl.resources.dbg import debugout

def update(ctx, path, verbose):
    debug = ctx.obj['debug']
    config_dir = os.path.expanduser(ctx.obj['config-dir'])
    if not os.path.exists(config_dir):
        os.makedirs(config_dir)
        if debug: debugout("created config dir: %s" % config_dir)
    cfg_file_path = os.path.join(config_dir, 'energy-dashboard-client.config')
    if os.path.exists(cfg_file_path):
        config = Config.load(cfg_file_path)
        if debug: debugout("loaded config file: %s" % cfg_file_path)
    else:
        config = Config()
        if debug: click.echo("loaded empty config")
    # override prev values
    config._debug   = verbose
    config._ed_path = path
    config.save()

def show(ctx):
    return Config.from_ctx(ctx)

class Config():
    M_ED_PATH       = 'ed_path'
    M_CFG_FILE      = 'cfg_file'
    M_DEBUG         = 'debug'
    DEF_CFG_PATH    = '~/.config'
    DEF_CFG_FILE    = 'energy-dashboard-client.config'
    DEF_ED_PATH     = '../energy-dashboard'
    def __init__(self, ed_path, cfg_file, debug):
        """
        """
        self._ed_path    = os.path.abspath(os.path.expanduser(ed_path   or  Config.DEF_ED_PATH))
        self._cfg_file   = os.path.abspath(os.path.expanduser(cfg_file  or  os.path.join(Config.DEF_CFG_PATH, Config.DEF_CFG_FILE)))
        self._debug      = debug or False

    def save(self) -> None:
        with open(self._cfg_file, 'w') as outfile:
            json.dump(self.to_map(), outfile, indent=4, sort_keys=True)

    def to_map(self):
        m               = {}
        m[Config.M_ED_PATH]   = self._ed_path
        m[Config.M_CFG_FILE]  = self._cfg_file
        m[Config.M_DEBUG]     = self._debug
        return m

    def from_map(m):
        ed_path    = m.get(Config.M_ED_PATH,     None)
        cfg_file   = m.get(Config.M_CFG_FILE,    None)
        debug      = m.get(Config.M_DEBUG,       None)
        return Config(ed_path, cfg_file, debug)

    def load(f:str):
        with open(f, 'r') as json_cfg_file:
            m = json.load(json_cfg_file)
            return Config.from_map(m)

    def from_ctx(ctx):
        cfg = Config.load(os.path.join(os.path.expanduser(ctx.obj['config-dir']), Config.DEF_CFG_FILE))
        return cfg

    def ed_path(self):
        return self._ed_path

    def cfg_file(self):
        return self._cfg_file

    def debug(self):
        return self._debug
    
    def __repr__(self):
        return json.dumps(self.to_map(), indent=4, sort_keys=True)

