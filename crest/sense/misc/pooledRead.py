#!/usr/bin/env python
__author__ = 'chriswhsu'

import datetime
import ConfigParser
import os
import logging
import threading
from time import sleep
import uuid
import pytz
import numpy
from cassandra.cluster import Cluster
from multiprocessing.pool import ThreadPool, TimeoutError



log = logging.getLogger()
log.setLevel('INFO')
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

    utc_date = datetime.datetime(2013, 10, 13, 0, 0, 0, 0, utc)

    mylist = {'10000000-0000-0000-0000-00000000094e', '10000001-0000-0000-0000-0000000008b8',
              '10000002-0000-0000-0000-0000000008b9', '10000003-0000-0000-0000-0000000008ba',
              '10000004-0000-0000-0000-0000000008d4', '10000005-0000-0000-0000-0000000008d8'}

    session = cluster.connect()

    log.info("setting keyspace...")
    session.set_keyspace(KEYSPACE)

    query = ("SELECT actpower FROM data where device_id = ? and day in (?)")
    prepared = session.prepare(query)
    pool = ThreadPool(processes=len(mylist))
    r = range(len(mylist))
    results =[]
    x = 0

    for myuuid in mylist:

        r[x] = pool.apply_async(func=runquery, args=(session, prepared, uuid.UUID(myuuid), utc_date))
        x += 1

    log.info("done spawning threads")

    #sleep(10)
    for t in r:
        results += t.get(timeout=5000)



    log.info('We got %s rows' % len(results))

    power = [row.actpower for row in results]

    log.info("Min Power: %d" % numpy.min(power))
    log.info("Max Power: %d" % numpy.max(power))
    log.info("Mean Power: %d" % numpy.mean(power))
    log.info("--------")


    log.info("done waiting for execution")


if __name__ == "__main__":
    main()
