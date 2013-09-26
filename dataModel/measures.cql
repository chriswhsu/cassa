use sense;

drop table if exists measures;

CREATE TABLE measures (
  name text,
  description text,
  uom text,
  datatype text,
  PRIMARY KEY (name)
) WITH
  compression={'sstable_compression': 'SnappyCompressor'};
