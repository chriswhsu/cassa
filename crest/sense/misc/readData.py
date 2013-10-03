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
log.setLevel('DEBUG')
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

    myuuid = uuid.UUID('c37d661d-7e61-49ea-96a5-68c34e83db3a')
    myuuid2 = uuid.UUID('a37d661d-7e61-49ea-96a5-68c34e83db3a')
    uuids = (myuuid, myuuid2)

    utc = pytz.utc

    utc_date = datetime.datetime(2013, 9, 27, 0, 0, 0, 0, utc)
    utc_date2 = datetime.datetime(2013, 9, 27, 0, 0, 0, 0, utc)

    mydates = (utc_date, utc_date2)

    query = ("SELECT actenergy, tp FROM data where device_id = ? and day = ?")

    prepared = session.prepare(query)

    future = session.execute_async(prepared.bind([myuuid, utc_date]))

    try:
        rows = future.result()
        print ('We got %s rows' % len(rows))
    except Exception:
        log.exeception()

    energy = [row.actenergy for row in rows]

    print numpy.max(energy)
    print numpy.mean(energy)
    log.info("done")

    session.shutdown()


if __name__ == "__main__":
    main()
