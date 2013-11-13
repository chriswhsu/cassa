#!/usr/bin/env python
THREADS = 6
__author__ = 'chriswhsu'

import datetime
import ConfigParser
import os
import logging
import uuid
from multiprocessing.pool import ThreadPool

import pytz
import numpy
from cassandra.cluster import Cluster
from dateutil import rrule




log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

Config = ConfigParser.ConfigParser()

#  look for config file in same directory as executable .py file.
Config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), '../sense.cnf'))

KEYSPACE = Config.get("Cassandra", "Keyspace")


def runquery(session, prepared, myuuid, utcdate):
    future = session.execute_async(prepared.bind([myuuid, utcdate]))

    log.info("start")

    return future.result()


def main():
    cluster = Cluster([Config.get("Cassandra", "Cluster")], port=Config.getint("Cassandra", "Port"))

    utc = pytz.utc

    utc_date_start = datetime.datetime(2013, 11, 5, 0, 0, 0, 0, utc)

    utc_date_end = datetime.datetime(2013, 11, 8, 0, 0, 0, 0, utc)

    #mylist = {'10000000-0000-0000-0000-0000000008b8'}


    mylist = {'10000000-0000-0000-0000-0000000008b8'}

    session = cluster.connect()

    log.info("setting keyspace...")
    session.set_keyspace(KEYSPACE)

    query = ("SELECT tp, actpower FROM data where device_id = ? and day in (?)")
    prepared = session.prepare(query)
    pool = ThreadPool(processes=THREADS)
    r = []
    results = []
    x = 0

    for myuuid in mylist:
        for the_date in list(rrule.rrule(rrule.DAILY, count=(utc_date_end - utc_date_start).days + 1, dtstart=utc_date_start)):
            r.append(pool.apply_async(func=runquery, args=(session, prepared, uuid.UUID(myuuid), the_date)))
            x += 1

    log.info("done spawning threads")

    for t in r:
        results += t.get(timeout=5000)
        log.info("Got a response")
        log.info('We have %s rows' % len(results))

    if results:

        power = [row.actpower for row in results]

        log.info("Min Power: %d" % numpy.min(power))
        log.info("Max Power: %d" % numpy.max(power))
        log.info("Mean Power: %d" % numpy.mean(power))
        log.info("--------")
    else:
        log.info("No Results")

    log.info("done waiting for execution")


if __name__ == "__main__":
    main()
