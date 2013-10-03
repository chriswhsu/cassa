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

from cassandra.cluster import Cluster

KEYSPACE = "testks"


def main():
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()

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
        session.execute(prepared.bind(('10001', day, tp, {'temp': 71}, '')))
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