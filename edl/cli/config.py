import os
import json
from edl.resources.dbg import debugout

class Config():
    M_ED_PATH       = 'ed_path'
    M_CFG_FILE      = 'cfg_file'
    M_DEBUG         = 'debug'
    DEF_CFG_PATH    = '~/.config/energy-dashboard'
    DEF_CFG_FILE    = 'energy-dashboard-client.config'
    DEF_ED_PATH     = 'energy-dashboard'
    def __init__(self, ed_path, cfg_file, debug):
        """
        """
        self.ed_path    = os.path.abspath(os.path.expanduser(ed_path   or  Config.DEF_ED_PATH))
        self.cfg_file   = os.path.abspath(os.path.expanduser(cfg_file  or  os.path.join(Config.DEF_CFG_PATH, Config.DEF_CFG_FILE)))
        self.debug      = debug or False

    def save(self) -> None:
        with open(self.cfg_file, 'w') as outfile:
            json.dump(self.to_map(), outfile, indent=4, sort_keys=True)

    def to_map(self):
        m                       = {}
        m[Config.M_ED_PATH]     = self.ed_path
        m[Config.M_CFG_FILE]    = self.cfg_file
        m[Config.M_DEBUG]       = self.debug
        return m

    def from_map(m):
        ed_path    = m.get(Config.M_ED_PATH,     None)
        cfg_file   = m.get(Config.M_CFG_FILE,    None)
        debug      = m.get(Config.M_DEBUG,       None)
        return Config(ed_path, cfg_file, debug)

    def __repr__(self):
        return json.dumps(self.to_map(), indent=4, sort_keys=True)

def load(config_dir=None, config_file_name=None):
    path        = os.path.abspath(os.path.expanduser(config_dir or Config.DEF_CFG_PATH))
    cfg_file    = config_file_name or Config.DEF_CFG_FILE
    return create(os.path.join(path, cfg_file))

def create(f:str):
    with open(f, 'r') as json_cfg_file:
        m = json.load(json_cfg_file)
        return Config.from_map(m)

def update(debug, config_dir, path, verbose):
    if not os.path.exists(config_dir):
        os.makedirs(config_dir)
        if debug: debugout("created config dir: %s" % config_dir)
    cfg_file_path = os.path.join(config_dir, 'energy-dashboard-client.config')
    if os.path.exists(cfg_file_path):
        config = Config.load(cfg_file_path)
        if debug: debugout("loaded config file: %s" % cfg_file_path)
    else:
        config = Config()
        if debug: debugout("loaded empty config")
    # override prev values
    config.debug   = verbose
    config.ed_path = path
    config.save()
    return config
