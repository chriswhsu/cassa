#!/usr/bin/env python
TIMEOUT = 10  # seconds

__author__ = 'chriswhsu'
import random
import datetime
import logging
import uuid
import sys
import os
from multiprocessing.pool import ThreadPool, TimeoutError
from time import sleep
from time import time

import pytz
from cassandra.cluster import Cluster, NoHostAvailable


def restart():
    print ('restarting program')
    print (sys.argv)
    python = sys.executable
    os.execl(python, python, *sys.argv)
    exit()


print ('0.10')
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)
print ('0.20')
#create utc datetime so we can remove time portion.
utc = pytz.utc


def get_connections():
    keyspace = "sense"
    cluster = Cluster(['128.32.189.230'], port=9042)
    session = cluster.connect()
    session.set_keyspace(keyspace)

    prepared = session.prepare("""
                            Insert into data (device_id, day, tp, actenergy, actpower, aparpower)
                            VALUES (?, ?, ?, ?, ?, ?)
                            """)
    log.info('finished get_connections')
    return cluster, session, prepared


def main(argv):

    def parse_write():
        ts = time()
        power = random.uniform(1000, 1000000)
        apparentpower = random.uniform (1000, 100000)
        energy = random.uniform (1000000, 10000000)

        utc_datetime = datetime.datetime.fromtimestamp(ts, utc)
        utc_date = utc_datetime.replace(hour=0, minute=0, second=0, microsecond=0)

        fullprefix = 10000000 * prefix

        for p in range(fullprefix, fullprefix + insertcount + 1):
            dev_uuid = uuid.UUID(str(p) + '-0000-0000-0000-000000000' + 'AAA')
            session.execute(prepared.bind((dev_uuid, utc_date, ts, energy, power, apparentpower)))
            log.debug("executed prepared statements")

        log.info("finished parse_write")

    log.info('STARTING in main')

    prefix = int(argv[0])
    insertcount = int(argv[1])

    pool = ThreadPool(processes=3)

    mycon = pool.apply_async(get_connections)
    try:
        cluster, session, prepared = mycon.get(TIMEOUT)
    except TimeoutError:
        log.info('timeout creation connections.')
        restart()

    except NoHostAvailable:
        sleep(10)
        restart()

    while True:

        parse_write()


if __name__ == "__main__":
    main(sys.argv[1:])
