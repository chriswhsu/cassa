#!/usr/bin/env python
__author__ = 'chriswhsu'

import logging


log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

from cassandra.cluster import Cluster
from cassandra.query import ValueSequence


KEYSPACE = "mykeyspace"

cluster = Cluster(['localhost'])
session = cluster.connect()

log.info("setting keyspace...")
session.set_keyspace(KEYSPACE)

name1 = 'john'
name2 = 'cwhsu'

names = (name1,name2)

query = "SELECT * FROM users WHERE user_id in (?)"

prepared = session.prepare(query)

future = session.execute_async(prepared.bind(ValueSequence(names)))

try:
    rows = future.result()
except Exception:
    log.exeception()

session.shutdown()
