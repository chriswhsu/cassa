#!/usr/bin/env python

__author__ = 'chriswhsu'

import logging
from time import time
import datetime
import uuid
import pytz

log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

from cassandra.cluster import Cluster

KEYSPACE = "sense"

cluster = Cluster(['128.32.189.129'], port=7902)
session = cluster.connect()

log.info("setting keyspace...")
session.set_keyspace(KEYSPACE)

prepared = session.prepare("""
          insert into data (device_id, day, tp, geohash, actEnergy)
           values
          (?, ?, ?, ?, ?)
         """)

log.info("created prepared statements")

utc = pytz.utc

for x in range(10000):
    ts = time()

    utc_datetime = datetime.datetime.fromtimestamp(ts, utc)
    utc_date = utc_datetime.replace(hour=0, minute=0, second=0, microsecond=0)

    session.execute(prepared.bind((
        uuid.UUID('c37d661d-7e61-49ea-96a5-68c34e83db3a'), utc_date, ts, None,
        x)))

log.info("completed inserts")

# session.execute("DROP KEYSPACE " + KEYSPACE)

session.shutdown()
