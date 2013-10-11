#!/usr/bin/env python
__author__ = 'chriswhsu'

import uuid
import datetime
import ConfigParser
import os

import pytz
import numpy
from cassandra.cluster import Cluster

import logging


log = logging.getLogger()
log.setLevel('INFO')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)


Config = ConfigParser.ConfigParser()

#  look for config file in same directory as executable .py file.
Config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), '../sense.cnf'))

KEYSPACE = Config.get("Cassandra", "Keyspace")


def main():
    cluster = Cluster([Config.get("Cassandra", "Cluster")], port=Config.getint("Cassandra", "Port"))
    session = cluster.connect()

    log.info("setting keyspace...")
    session.set_keyspace(KEYSPACE)

    utc = pytz.utc

    utc_date = datetime.datetime(2013, 10, 9, 0, 0, 0, 0, utc)
    #utc_date2 = datetime.datetime(2013, 10, 10, 0, 0, 0, 0, utc)

    query = ("SELECT actpower FROM data where device_id = ? and day in (?)")

    prepared = session.prepare(query)

    myuuid = uuid.UUID('10000000-0000-0000-0000-00000000094e')

    future = session.execute_async(prepared.bind([myuuid, utc_date]))
    log.info("start")
    rows = future.result()
    log.info ('We got %s rows' % len(rows))

    power = [row.actpower for row in rows]

    log.info("Min Power: %d"%numpy.min(power))
    log.info("Max Power: %d"%numpy.max(power))
    log.info("Mean Power: %d"%numpy.mean(power))
    log.info("done")


    myuuid = uuid.UUID('10000001-0000-0000-0000-0000000008b8')

    future = session.execute_async(prepared.bind([myuuid, utc_date]))
    log.info("start")
    rows = future.result()
    log.info ('We got %s rows' % len(rows))

    power = [row.actpower for row in rows]

    log.info("Min Power: %d"%numpy.min(power))
    log.info("Max Power: %d"%numpy.max(power))
    log.info("Mean Power: %d"%numpy.mean(power))
    log.info("done")

    myuuid = uuid.UUID('10000002-0000-0000-0000-0000000008b9')

    future = session.execute_async(prepared.bind([myuuid, utc_date]))
    log.info("start")
    rows = future.result()
    log.info ('We got %s rows' % len(rows))

    power = [row.actpower for row in rows]

    log.info("Min Power: %d"%numpy.min(power))
    log.info("Max Power: %d"%numpy.max(power))
    log.info("Mean Power: %d"%numpy.mean(power))
    log.info("done")

    myuuid = uuid.UUID('10000003-0000-0000-0000-0000000008ba')

    future = session.execute_async(prepared.bind([myuuid, utc_date]))
    log.info("start")
    rows = future.result()
    log.info ('We got %s rows' % len(rows))

    power = [row.actpower for row in rows]

    log.info("Min Power: %d"%numpy.min(power))
    log.info("Max Power: %d"%numpy.max(power))
    log.info("Mean Power: %d"%numpy.mean(power))
    log.info("done")

    myuuid = uuid.UUID('10000004-0000-0000-0000-0000000008d4')

    future = session.execute_async(prepared.bind([myuuid, utc_date]))
    log.info("start")
    rows = future.result()
    log.info ('We got %s rows' % len(rows))

    power = [row.actpower for row in rows]

    log.info("Min Power: %d"%numpy.min(power))
    log.info("Max Power: %d"%numpy.max(power))
    log.info("Mean Power: %d"%numpy.mean(power))
    log.info("done")

    myuuid = uuid.UUID('10000005-0000-0000-0000-0000000008d8')

    future = session.execute_async(prepared.bind([myuuid, utc_date]))
    log.info("start")
    rows = future.result()
    log.info ('We got %s rows' % len(rows))

    power = [row.actpower for row in rows]

    log.info("Min Power: %d"%numpy.min(power))
    log.info("Max Power: %d"%numpy.max(power))
    log.info("Mean Power: %d"%numpy.mean(power))
    log.info("done")


    session.shutdown()


if __name__ == "__main__":
    main()
