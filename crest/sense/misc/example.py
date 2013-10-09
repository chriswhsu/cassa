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
# from cassandra.io.libevreactor import LibevConnection
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

KEYSPACE = "test"

def main():
    cluster = Cluster(['128.32.189.228'],port=9042)
    # cluster.connection_class = LibevConnection
    session = cluster.connect()

    rows = session.execute("SELECT keyspace_name FROM system.schema_keyspaces")

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
    query = SimpleStatement("""
        INSERT INTO mytable (thekey, col1, timepoint)
        VALUES (%(key)s, %(a)s, %(b)s)
        """, consistency_level=ConsistencyLevel.ONE)

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
