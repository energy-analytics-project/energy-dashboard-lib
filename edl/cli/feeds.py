import os
from edl.resources import log

def list(logger, energy_dashboard_path):
    chlogger = logger.getChild(__name__)
    log.debug(chlogger, {
        "name"      : __name__,
        "method"    : "list",
        "path"      : energy_dashboard_path})
    return os.listdir(os.path.join(energy_dashboard_path, "data"))
