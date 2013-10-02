__author__ = 'chriswhsu'

import crest.senseWorker

sns = crest.senseWorker.SenseWorker(test = True)

sns.session.execute("""drop table if exists devices""")
sns.session.execute("""CREATE TABLE devices (
                         device_id uuid,
                         external_identifier text,
                         geohash text,
                         measures set<text>,
                         name text,
                         parent_device_id uuid,
                         tags map<text, text>,
                         PRIMARY KEY (device_id)
                       ) WITH
                         bloom_filter_fp_chance=0.010000 AND
                         caching='KEYS_ONLY' AND
                         comment='' AND
                         dclocal_read_repair_chance=0.000000 AND
                         gc_grace_seconds=864000 AND
                         index_interval=128 AND
                         read_repair_chance=0.100000 AND
                         replicate_on_write='true' AND
                         populate_io_cache_on_flush='false' AND
                         default_time_to_live=0 AND
                         speculative_retry='NONE' AND
                         memtable_flush_period_in_ms=0 AND
                         compaction={'class': 'SizeTieredCompactionStrategy'} AND
                         compression={'sstable_compression': 'SnappyCompressor'}""")


sns.session.execute("""CREATE INDEX external_id_ind ON devices (external_identifier)""")
sns.session.execute("""CREATE INDEX name_ind ON devices (name)""")
sns.session.execute("""CREATE INDEX geohash_ind ON devices (geohash)""")
sns.session.execute("""CREATE INDEX parent_device_id_ind ON devices (parent_device_id)""")