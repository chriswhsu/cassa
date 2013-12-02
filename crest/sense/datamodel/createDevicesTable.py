__author__ = 'chriswhsu'

from crest.sense.cassandraworker import CassandraWorker

sns = CassandraWorker(test=True)

sns.session.execute("""drop table devices""")
sns.log.info('dropped table devices.')


sns.session.execute("""CREATE TABLE devices (
                         device_uuid uuid,
                         external_identifier text,
                         geohash text,
                         latitude float,
                         longitude float,
                         measures set<text>,
                         name text,
                         parent_device_uuid uuid,
                         tags map<text, text>,
                         PRIMARY KEY (device_uuid)
                       ) WITH
                         compression={'sstable_compression': 'SnappyCompressor'};""")

sns.session.execute("""CREATE INDEX external_id_ind ON devices (external_identifier);""")

sns.session.execute("""CREATE INDEX name_ind ON devices (name);""")

sns.session.execute("""CREATE INDEX geohash_ind ON devices (geohash);""")

sns.session.execute("""CREATE INDEX parent_device_uuid_ind ON devices (parent_device_uuid);""")
