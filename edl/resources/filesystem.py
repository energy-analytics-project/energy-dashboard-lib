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

"""
filesystem.py : compute things like filenames
"""

import os
import pdb

BAD_S3_CHARS = ['&', '@', ':', ',', '$', '=', '+', '?', ';', ' ', '\\', '^', '>', '<', '{', '}', '[', ']', '%', '~', '|']
#PROTOCOLS = ["http://", "https://", "ftp://"]
# TODO: yes, there are replacement chars that are removed in the linter
# TODO: need to sort this out after the migration
COMMON_REPLACEMENTS = [
    ("http://oasis.caiso.com/oasisapi/", "oasis_"),
    ("http://zwrob.com/", ""),
    ("/", "_"),
    ("&", ","),
    ("SingleZip", "SZ"),
    ("queryname", "q"),
    ("startdatetime", "sdt"),
    ("enddatetime", "edt"),
    ("market_run_id", "mri"),
    ("version", "v")
]

def glob_dir(path, ending):
    """Returns files matching ending (simple glob), does NOT DO REGEX"""
    with os.scandir(path) as it:
        for entry in it:
            if entry.is_file() and entry.name.lower().endswith(ending.lower()):
                yield(entry.name)

def shrink_file_name(name, rtuples):
    """Shrink file name using provided replacements in rtuples"""
    for (a,b) in rtuples:
        name = name.replace(a,b)
    return name

def s3lint_file_name(name):
    """Replace characters known to break S3"""
    for c in BAD_S3_CHARS:
        name = name.replace(c, "_")
    return name

def url2filename(url, rtuples = COMMON_REPLACEMENTS, ending = ".zip"):
    """
    Download URLs may contain characters that are not OK on either
    the local filesystem or the remote store (S3). `url2filename`
    returns a filename that is safe for either.

    url     : url for the downloaded resource
    ending  : file name ending, like '.zip' for the created local file
    rtuples : list of (string, string) tuple replacements 
    """
    name = s3lint_file_name(shrink_file_name(url, rtuples))
    return "%s%s" % (name, ending)

def clean_legacy_filename(name, ending=".zip"):
    """
    Convert legacy file name to a properly linted name. 
    Note: name should end with 'ending' 
    """
    if name.endswith(ending):
        return "%s%s" % (s3lint_file_name(name[:-len(ending)]), ending)
    else:
        raise Exception("bad file name: expected name to end with '%s'; name: %s" % (ending, name))
