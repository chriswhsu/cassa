#!/usr/bin/env python
__author__ = 'chriswhsu'

import logging
import uuid
import datetime

import pytz
import numpy


log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

from cassandra.cluster import Cluster


KEYSPACE = "sense"


def main():
    # cluster = Cluster(['128.32.33.229'])
    cluster = Cluster(['128.32.189.129'], port=7902)
    session = cluster.connect()

    log.info("setting keyspace...")
    session.set_keyspace(KEYSPACE)

    myuuid = uuid.UUID('c37d661d-7e61-49ea-96a5-68c34e83db3a')

    utc = pytz.utc

    utc_date = datetime.datetime(2013, 9, 27, 0, 0, 0, 0, utc)


    query = ("SELECT actenergy FROM data where device_id = ? and day = ? order by tp desc limit 100000")

    prepared = session.prepare(query)

    future = session.execute_async(prepared.bind((myuuid, utc_date)))

    try:
        rows = future.result()
    except Exception:
        log.exeception()

    energy = [row.actenergy for row in rows]

    print numpy.max(energy)
    print numpy.mean(energy)
    log.info("done")

    session.shutdown()


if __name__ == "__main__":
    main()
