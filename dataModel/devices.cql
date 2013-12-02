CREATE TABLE devices (
  device_uuid uuid PRIMARY KEY,
  external_identifier text,
  geohash text,
  latitude float,
  longitude float,
  measures set<text>,
  name text,
  parent_device_uuid uuid,
  tags map<text, text>
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.000000 AND
  gc_grace_seconds=864000 AND
  read_repair_chance=0.100000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'SnappyCompressor'};

CREATE INDEX external_id_ind ON devices (external_identifier);

CREATE INDEX name_ind ON devices (name);

CREATE INDEX geohash_ind ON devices (geohash);

CREATE INDEX parent_device_uuid_ind ON devices (parent_device_uuid);
