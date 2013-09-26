use sense;

drop table if exists data;

CREATE TABLE data (
  device_id int,
  day timestamp,
  tp timestamp,
  geohash text,
  event list<text>,
  aparPower float,
  actPower float,
  actEnergy float,
  humidity float,
  temp float,
  pressure float,
  solar_rad float,
  wind_dir float,
  wind_speed float,
  rfid_scan boolean,
  PRIMARY KEY ((device_id,day),tp)
) WITH
  clustering order by (tp DESC) AND
  compression={'sstable_compression': 'SnappyCompressor'};
                                              

