import os
import json

def list(logger, energy_dashboard_path):
    lgr = logger.getChild(__name__)
    lgr.debug(json.dumps({
        "name"      : __name__,
        "method"    : "list",
        "path"      : energy_dashboard_path}))
    return os.listdir(os.path.join(energy_dashboard_path, "data"))
