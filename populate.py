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
from cassandra.query import SimpleStatement

import sys, getopt


KEYSPACE = "test"


def main(argv):

    nid = argv[0]
    count = int(argv[1])

    # cluster = Cluster(['128.32.33.229'])
    cluster = Cluster(['128.32.189.129'], port=7902)
    session = cluster.connect()

    log.info("setting keyspace...")
    session.set_keyspace(KEYSPACE)

    # prepared = session.prepare("""
    #     INSERT INTO mytable3 (theverylongkey, myfavoritecolumnone, veryprecisetimepoint)
    #     VALUES (?, ?, ?)
    #     """)
        
    prepared = session.prepare("""
        INSERT INTO mytable2 (thekey, col1, timepoint, value)
         VALUES (?, ?, ?,?)
         """)


    log.info("created prepared statements")
    for i in range(count):
        # log.info("inserting row %d" % i)
        # session.execute(query, dict(key="key%d" % i, a='cat', b=datetime.now()))
        # session.execute(prepared.bind(("key%d" % i, 'rat', datetime.now())))
        session.execute(prepared.bind((nid, "%d" % i, datetime.now(), i)))

        # session.execute(prepared2.bind((nid, "%d" % i, datetime.now())))

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
   main(sys.argv[1:])
