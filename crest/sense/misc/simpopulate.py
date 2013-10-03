__author__ = 'chriswhsu'
#!/usr/bin/env python

import logging
from time import sleep
import uuid

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
          insert into devices (device_id, geohash, name, external_identifier, measures, tags)
           values
          (?, ?, ?, ?, ?, ?)
         """)

log.info("created prepared statements")

# log.info("inserting row %d" % i)
# session.execute(query, dict(key="key%d" % i, a='cat', b=datetime.now()))
# session.execute(prepared.bind(("key%d" % i, 'rat', datetime.now())))
session.execute(prepared.bind((
    uuid.UUID('c37d661d-7e61-49ea-96a5-68c34e83db3a'), '9q9p3yyrn1', 'Acme1', '936',
    {'aparPower', 'actPower', 'actEnergy'},
    {'make': 'Acme'})))

session.execute(prepared.bind((
    uuid.UUID('7a6a3557-a23d-4e34-8b76-80b74490db70'), '9q9p3yyrn3', 'Acme2', '94e',
    {'aparPower', 'actPower', 'actEnergy'}, {'make': 'Acme'})))

session.execute(prepared.bind((
    uuid.UUID('c37d661d-7e61-49ea-96a5-68c34e83db3a'), '9q9p3yyrn1', 'Acme1', '936',
    {'aparPower', 'actPower', 'actEnergy'},
    {'make': 'Acme'})))

session.execute(prepared.bind((
    uuid.UUID('c37d661d-7e61-49ea-96a5-68c34e83db3a'), '9q9p3yyrn1', 'Acme1', '936',
    {'aparPower', 'actPower', 'actEnergy'},
    {'make': 'Acme'})))

session.execute(prepared.bind((
    uuid.UUID('c37d661d-7e61-49ea-96a5-68c34e83db3a'), '9q9p3yyrn1', 'Acme1', '936',
    {'aparPower', 'actPower', 'actEnergy'},
    {'make': 'Acme'})))


# session.execute(prepared2.bind((nid, "%d" % i, datetime.now())))

log.info("completed inserts")

# session.execute("DROP KEYSPACE " + KEYSPACE)

session.shutdown()
sleep(1)
