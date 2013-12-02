__author__ = 'chriswhsu'

from crest.sense.cassandraworker import CassandraWorker

sns = CassandraWorker(test=False)

#sns.session.execute("""drop table data""")
#sns.log.info('dropped table data.')

sns.session.execute("""CREATE TABLE data (
                          device_uuid uuid,
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
                          PRIMARY KEY ((device_uuid,day),tp)
                        ) WITH
                          clustering order by (tp DESC) AND
                          compression={'sstable_compression': 'SnappyCompressor', 'chunk_length_kb': '512'}
                    """)

sns.log.info('created table data.')