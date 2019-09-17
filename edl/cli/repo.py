# edc : Energy Dashboard Command Line Interface
# Copyright (C) 2019  Todd Greenwood-Geer (Enviro Software Solutions, LLC)
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from edl.resources.exec import runyield
from edl.resources import log
import logging
import os

def clone(logger, ed_path):
    cmd = "git clone https://github.com/energy-analytics-project/energy-dashboard.git"
    log.debug(logger, {
        "name"      : __name__,
        "method"    : "init",
        "path"      : ed_path,
        "cmd"       : cmd,
        })
    return runyield(cmd,ed_path)

def update(logger, ed_path):
    cmd = "git submodule update --init --recursive"
    log.debug(logger, {
        "name"      : __name__,
        "method"    : "update",
        "path"      : ed_path,
        "cmd"       : cmd,
        })
    return runyield(cmd,ed_path)
