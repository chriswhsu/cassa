__author__ = 'chriswhsu'

from crest.sense.senseworker import SenseWorker

sns = SenseWorker(test=False)

# sns.session.execute("""drop table if exists data""")
# sns.log.info('dropped table data.')

sns.session.execute("""CREATE TABLE data (
                          device_id uuid,
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
                          compression={'sstable_compression': 'SnappyCompressor'}
                    """)

sns.log.info('created table data.')