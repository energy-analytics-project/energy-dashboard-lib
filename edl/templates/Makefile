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

.PHONE: all
all: help

.PHONY: help
help:
	# -----------------------------------------------------------------------------
	# Processor for {{DATA_SOURCE_NAME}}
	#
	# Targets:
	#
	#     proc    : invoke all targets [down,unzip,injest,save]
	#     down    : download zip files 
	#     unzip   : unzip zip files
	#     injest  : injest xml files into sqlite db
	#     save    : commit data to store to repo
	#
	# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# TARGETS
# -----------------------------------------------------------------------------
.PHONY: setup
setup:  
	pipenv install requests

.PHONY: proc
proc:  down unzip injest save

.PHONY: down
down:  
	src/10_down.py

.PHONY: unzip
unzip:  
	src/20_unzp.py

.PHONY: injest
injest:  
	src/30_inse.py

.PHONY: save
save:  
	src/40_save.sh
