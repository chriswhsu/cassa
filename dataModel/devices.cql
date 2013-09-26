use wherewhenwhat;

drop table if exists devices; 

CREATE TABLE devices (
  device_id int,
  parent_device_id int,
  geohash text,
  name text,
  measures set<text>,
  tags map<text, text>,
  PRIMARY KEY (device_id)
) WITH
  compression={'sstable_compression': 'SnappyCompressor'};
