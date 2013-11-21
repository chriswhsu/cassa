__author__ = 'chriswhsu'

import time
import datetime

import pytz


def gmt_date():
    ts = time.time()
    utc = pytz.utc
    utc_datetime = datetime.datetime.fromtimestamp(ts, utc)
    return utc_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
