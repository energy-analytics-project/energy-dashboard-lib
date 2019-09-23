from edl.resources import exec as fsexec
from edl.resources import filesystem
from edl.resources import log
import logging
import os

def git_add_and_commit(logger, resource):
    """
    """
    for cmd in ["git add *", "git commit -am 'update state'", "git push"]:
        log.debug(logger, {
            "name"      : __name__,
            "method"    : "git_add_and_commit",
            "resource"  : resource,
            "cmd"       : cmd,
            })
        yield fsexec.runyield(cmd, os.curdir)

