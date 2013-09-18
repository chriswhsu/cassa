__author__ = 'chriswhsu'
#!/usr/bin/env python

import logging
from time import sleep
from datetime import datetime

recreate = True
log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster

KEYSPACE = "testks"

def main():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    if recreate:
        rows = session.execute("SELECT keyspace_name FROM system.schema_keyspaces")
        if KEYSPACE in [row[0] for row in rows]:
            log.info("dropping existing keyspace...")
            session.execute("DROP KEYSPACE " + KEYSPACE)

        log.info("creating keyspace...")
        session.execute("""
            CREATE KEYSPACE %s
            WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
            """ % KEYSPACE)

        log.info("setting keyspace...")
        session.set_keyspace(KEYSPACE)

        log.info("creating table...")
        session.execute("""
                        CREATE TABLE ddata (
                          deviceID text,
                          day timestamp,
                          timepoint timestamp,
                          feeds map<text,int>,
                          event list<text>,
                          PRIMARY KEY ((deviceID,day),timepoint)
                        ) WITH
                          clustering order by (timepoint DESC) AND
                          bloom_filter_fp_chance=0.010000 AND
                          caching='KEYS_ONLY' AND
                          comment='devices' AND
                          dclocal_read_repair_chance=0.000000 AND
                          gc_grace_seconds=864000 AND
                          read_repair_chance=0.100000 AND
                          replicate_on_write='true' AND
                          populate_io_cache_on_flush='false' AND
                          compaction={'class': 'SizeTieredCompactionStrategy'} AND
                          compression={'sstable_compression': 'SnappyCompressor'}
            """)
        log.info("created table")

    prepared = session.prepare("""
        Insert into ddata (deviceID,day,timepoint,feeds,event)
        VALUES (?, ?, ?, ?, ?)
        """)

    log.info("created prepared statements")
    for i in range(5):

        tp = datetime.now()
        day = tp.replace(hour=0, minute=0, second=0, microsecond=0)

        # log.info("inserting row %d" % i)
        # session.execute(query, dict(key="key%d" % i, a='cat', b=datetime.now()))
        # session.execute(prepared.bind(("key%d" % i, 'rat', datetime.now())))
        session.execute(prepared.bind(('10001', day , tp,{'temp':71},'')))
        sleep(1)

    log.info("completed inserts")
    # future = session.execute_async("SELECT * FROM mytable")
    #
    # try:
    #     rows = future.result()
    # except Exception:
    #     log.exeception()
    #
    # for row in rows:
    #     print row
    #
    # log.info("done")

    # session.execute("DROP KEYSPACE " + KEYSPACE)

    session.shutdown()
    sleep(1)


if __name__ == "__main__":
    main()