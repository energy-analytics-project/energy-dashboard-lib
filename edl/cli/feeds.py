from edl.cli.config import Config
from edl.resources.dbg import debugout
import os

def list(ctx):
    cfg = Config.from_ctx(ctx)
    items = os.listdir(os.path.join(cfg.ed_path(), "data"))
    return items
