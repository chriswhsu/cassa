#!/usr/bin/env python
__author__ = 'chriswhsu'

import uuid
import datetime
import ConfigParser
import os
import logging
import thread
from time import sleep

import pytz
import numpy
from cassandra.cluster import Cluster


log = logging.getLogger()
log.setLevel('INFO')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

Config = ConfigParser.ConfigParser()

#  look for config file in same directory as executable .py file.
Config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), '../sense.cnf'))

KEYSPACE = Config.get("Cassandra", "Keyspace")


def runquery(cluster, myuuid, utcdate):
    session = cluster.connect()

    log.info("setting keyspace...")
    session.set_keyspace(KEYSPACE)

    query = ("SELECT actpower FROM data where device_id = ? and day in (?)")

    prepared = session.prepare(query)

    future = session.execute_async(prepared.bind([myuuid, utcdate]))

    log.info("start")

    def log_results(rows):
        log.info('We got %s rows' % len(rows))

        power = [row.actpower for row in rows]

        log.info("Min Power: %d" % numpy.min(power))
        log.info("Max Power: %d" % numpy.max(power))
        log.info("Mean Power: %d" % numpy.mean(power))
        log.info("done")

    def log_error(exc):
        log.error("Operation failed: %s", exc)

    future.add_callbacks(log_results, log_error)


def main():
    cluster = Cluster([Config.get("Cassandra", "Cluster")], port=Config.getint("Cassandra", "Port"))

    utc = pytz.utc

    utc_date = datetime.datetime(2013, 10, 9, 0, 0, 0, 0, utc)

    myuuid = uuid.UUID('10000000-0000-0000-0000-0000000008b8')
    thread.start_new_thread(runquery,(cluster, myuuid, utc_date))

    myuuid = uuid.UUID('10000001-0000-0000-0000-0000000008b8')
    thread.start_new_thread(runquery,(cluster, myuuid, utc_date))

    myuuid = uuid.UUID('10000002-0000-0000-0000-0000000008b9')
    thread.start_new_thread(runquery,(cluster, myuuid, utc_date))

    myuuid = uuid.UUID('10000003-0000-0000-0000-0000000008ba')
    thread.start_new_thread(runquery,(cluster, myuuid, utc_date))

    myuuid = uuid.UUID('10000004-0000-0000-0000-0000000008d4')
    thread.start_new_thread(runquery,(cluster, myuuid, utc_date))

    myuuid = uuid.UUID('10000005-0000-0000-0000-0000000008d8')
    thread.start_new_thread(runquery,(cluster, myuuid, utc_date))

    sleep(20)


if __name__ == "__main__":
    main()
