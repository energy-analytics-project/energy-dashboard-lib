from edl.resources import filesystem
from edl.resources import log
import logging
import os
import subprocess
import sys

def git_add_and_commit(logger, resource, db_dir, state_file):
    """
    """
    db_files = filesystem.glob_dir(db_dir, ".db")
    with open(state_file, 'w') as f:
        for dbf in db_files:
            f.write("%s\n" % dbf)

    def do_git(cmd):
        r = subprocess.run(cmd, shell=True)
        log.info(logger, {
            "name"      : __name__,
            "method"    : "git_add_and_commit",
            "resource"  : resource,
            "cmd"       : cmd,
            })

    do_git(["git", "add",  "*"])
    do_git(["git", "commit -am", "\"update state\""])
    do_git(["git", "push"])
