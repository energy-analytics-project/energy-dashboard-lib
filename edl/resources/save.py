from edl.resources import log
import logging
import os
import subprocess

def git_add_and_commit(logger, resource):
    """
    """
    for cmd in ["git add *", "git commit -am 'update state'", "git push"]:
        proc = subprocess.run(cmd, cwd=os.curdir, shell=True, stderr=subprocess.STDOUT)
        log.info(logger, {
            "name"      : __name__,
            "method"    : "git_add_and_commit",
            "resource"  : resource,
            "cmd"       : cmd,
            "stdout"    : proc.stdout,
            "stderr"    : proc.stderr,
            "returncode"   : proc.returncode,
            })
