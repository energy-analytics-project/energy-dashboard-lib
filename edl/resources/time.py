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
