use sense;

drop table if exists devices; 

CREATE TABLE devices (
  device_id int,
  parent_device_id int,
  geohash text,
  name text,
  external_identifier text,
  measures set<text>,
  tags map<text, text>,
  PRIMARY KEY (device_id)
) WITH
  compression={'sstable_compression': 'SnappyCompressor'};


create index geohash_ind on devices(geohash);
create index external_id_ind on devices(external_identifier);