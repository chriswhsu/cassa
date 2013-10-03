__author__ = 'chriswhsu'
#!/usr/bin/env python

import logging
from time import sleep
import uuid
import ConfigParser

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
          insert into devices (device_id, geohash, name, external_identifier, measures, tags, parent_device_id)
           values
          (?, ?, ?, ?, ?, ?, ?)
         """)

log.info("created prepared statements")

# log.info("inserting row %d" % i)
# session.execute(query, dict(key="key%d" % i, a='cat', b=datetime.now()))
# session.execute(prepared.bind(("key%d" % i, 'rat', datetime.now())))
session.execute(prepared.bind((
    uuid.UUID('c37d661d-7e61-49ea-96a5-68c34e83db3a'), '9q9p3yyrn1', 'Acme1', '936',
    {'aparPower', 'actPower', 'actEnergy'},
    {'make': 'Acme'},
    None)))

session.execute(prepared.bind((
    uuid.UUID('7a6a3557-a23d-4e34-8b76-80b74490db70'), '9q9p3yyrn3', 'Acme2', '94e',
    {'aparPower', 'actPower', 'actEnergy'},
    {'make': 'Acme'},
    None)))

session.execute(prepared.bind((
    uuid.UUID('e0e6c13e-4686-4b75-b62a-e7b406baa36e'), '9q9p3yyrn5', 'Acme3', '8b9',
    {'aparPower', 'actPower', 'actEnergy'},
    {'make': 'Acme'},
    None)))

session.execute(prepared.bind((
    uuid.UUID('dd1b0837-146e-4926-9a6a-155e2ffb885a'), None, 'WeatherSation', None,
    None,
    {'make': 'Davis', 'model': 'VantagePro 2'},
    None)))


session.execute(prepared.bind((
    uuid.UUID('22c06d59-f4f2-4e77-9e4e-9df043ccf5df'), '9q9p3yyrna', 'Receiving Console', None,
    {'humidity','temp'},
    None,
    uuid.UUID('dd1b0837-146e-4926-9a6a-155e2ffb885a')
    )))

session.execute(prepared.bind((
    uuid.UUID('695118d1-b5ca-4410-8790-466bcdfa573b'), '9q9p3yyrnb', 'Outdoor Unit', None,
    {'humidity','temp','pressure','solar_rad','wind_dir','wind_speed'},
    None,
    uuid.UUID('dd1b0837-146e-4926-9a6a-155e2ffb885a')
    )))


session.execute(prepared.bind((
    uuid.UUID('84d1a535-bdef-4c72-8972-449287b6b6e8'), '9q9p3yyrn1', 'RFID Reader', '8b9',
    {'rfid_scan'},
    None,
    None)))


log.info("completed inserts")

# session.execute("DROP KEYSPACE " + KEYSPACE)

session.shutdown()
sleep(1)
