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
time.py : compute things like date ranges
"""
import datetime

def day_range(start_date, end_date):
    return sorted([start_date + datetime.timedelta(n) 
        for n in range(int ((end_date - start_date).days))])

def day_range_to_today(start_date):
    return day_range(start_date, datetime.datetime.now().date())

def range_pairs(dates):
    start = dates[:-1]
    end = dates[1:]
    return zip(start,end)
