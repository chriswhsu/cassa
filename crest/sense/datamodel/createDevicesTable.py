import logging
from cassandra.cluster import Cluster
from time import sleep


log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

cluster = Cluster(['128.32.189.228','128.32.189.229','128.32.189.230'])
session = cluster.connect()

session.execute("""use test;""")
#session.execute("""drop table test.devices;""")
log.info('dropped table devices.')

session.execute("""CREATE TABLE devices (
                         device_uuid uuid,
                         external_identifier text,
                         geohash text,
                         latitude float,
                         longitude float,
                         measures set<text>,
                         name text,
                         parent_device_id uuid,
                         tags map<text, text>,
                         PRIMARY KEY (device_uuid)
                       ) WITH
                         compression={'sstable_compression': 'SnappyCompressor'} USING CONSISTENCY ALL;""")

session.execute("""CREATE INDEX external_id_ind ON devices (external_identifier) USING CONSISTENCY ALL;""")

session.execute("""CREATE INDEX name_ind ON devices (name) USING CONSISTENCY ALL;""")

session.execute("""CREATE INDEX geohash_ind ON devices (geohash) USING CONSISTENCY ALL;""")

session.execute("""CREATE INDEX parent_device_id_ind ON devices (parent_device_id) USING CONSISTENCY ALL;""")
