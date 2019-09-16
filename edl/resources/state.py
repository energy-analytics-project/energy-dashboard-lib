# edl : common library for the energy-dashboard tool-chain
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

import os
import logging
from edl.resources import filesystem

def update(generator, state_file):
    for item in generator:
        with open(state_file, "a") as f:
            f.write("%s\n" % item)

def new_files(resource_name, state_file, path, ending):
    """Return a list of files that are not present in the state file""" 
    processed_file_set = set()
    logging.info({
        "src":resource_name, 
        "action":"new_%s_files" % ending})
    if os.path.exists(state_file):
        with open(state_file, 'r') as m:
            processed_file_set = set([l.rstrip() for l in m])
    all_file_set = set(filesystem.glob_dir(path, ending))
    new_file_set = all_file_set - processed_file_set
    logging.info({
        "src":resource_name, 
        "action":"new_%s_files" % ending, 
        "new_file_set_count":len(new_file_set), 
        "all_file_set_count": len(all_file_set), 
        "processed_file_set_count": len(processed_file_set)})
    return list(new_file_set)
