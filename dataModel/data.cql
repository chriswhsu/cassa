use wherewhenwhat;

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
  PRIMARY KEY ((deviceID,day),timepoint)
) WITH
  clustering order by (timepoint DESC) AND
  compression={'sstable_compression': 'SnappyCompressor'};
                                              

