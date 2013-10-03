__author__ = 'chriswhsu'
#!/usr/bin/env python

import logging
from time import sleep
from datetime import datetime


log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster

KEYSPACE = "testkeyspace"

def main():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()

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
        CREATE TABLE mytable (
            thekey text,
            col1 text,
            timepoint timestamp,
            PRIMARY KEY (thekey, col1)
        )
        """)
    log.info("created table")

    prepared = session.prepare("""
        INSERT INTO mytable (thekey, col1, timepoint)
        VALUES (?, ?, ?)
        """)

    log.info("created prepared statements")
    for i in range(100000):
        # log.info("inserting row %d" % i)
        # session.execute(query, dict(key="key%d" % i, a='cat', b=datetime.now()))
        # session.execute(prepared.bind(("key%d" % i, 'rat', datetime.now())))
        session.execute(prepared.bind(("key1", "rat%d" % i, datetime.now())))


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