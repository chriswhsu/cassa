#!/usr/bin/env python
TIMEOUT = 100

__author__ = 'chriswhsu'
import httplib
import json
import datetime
import logging
import uuid
import sys
from multiprocessing.pool import ThreadPool, TimeoutError

import pytz

from cassandra.cluster import Cluster


def main(argv):
    def get_json():

        log.info('t-getting data with web request')
        conn.request("GET", "/data/+")
        r = conn.getresponse()
        log.debug('t-got response: ' + str(r.status) + ' ' + r.reason)
        dat_str = r.read()
        results = json.loads(dat_str)
        log.info('t-parsed into json.')
        return results

    def parse_write():
        for i in reading['/costas_acmes']['Contents']:

            i = str(i)

            log.debug("processing %s", i)

            v = {'ap': [False, False], 'tp': [False, False], 'te': [False, False]}
            t = {'ap': [False, False], 'tp': [False, False], 'te': [False, False]}

            index1 = '/costas_acmes/' + i + '/apparent_power'
            index2 = '/costas_acmes/' + i + '/true_power'
            index3 = '/costas_acmes/' + i + '/true_energy'

            if index1 in reading:
                t['ap'][0] = reading[index1]['Readings'][0][0]
                v['ap'][0] = reading[index1]['Readings'][0][1]
                t['ap'][1] = reading[index1]['Readings'][1][0]
                v['ap'][1] = reading[index1]['Readings'][1][1]

            if index2 in reading:
                t['tp'][0] = reading[index2]['Readings'][0][0]
                v['tp'][0] = reading[index2]['Readings'][0][1]
                t['tp'][1] = reading[index2]['Readings'][1][0]
                v['tp'][1] = reading[index2]['Readings'][1][1]

            if index3 in reading:
                t['te'][0] = reading[index3]['Readings'][0][0]
                v['te'][0] = reading[index3]['Readings'][0][1]
                t['te'][1] = reading[index3]['Readings'][1][0]
                v['te'][1] = reading[index3]['Readings'][1][1]

            if t['ap'][1] > update_time[i]:
                update_time[i] = t['ap'][1]
                # update to database
                for reading_id in range(0, 2):
                    ts = (int(t['ap'][reading_id]) / 1000)
                    power = v['tp'][reading_id]
                    apparentpower = v['ap'][reading_id]
                    energy = v['te'][reading_id]

                    utc_datetime = datetime.datetime.fromtimestamp(ts, utc)
                    utc_date = utc_datetime.replace(hour=0, minute=0, second=0, microsecond=0)

                    fullprefix = 10000000 * prefix

                    for p in range(fullprefix, fullprefix + insertcount + 1):
                        dev_uuid = uuid.UUID(str(p) + '-0000-0000-0000-000000000' + i)
                        session.execute(prepared.bind((dev_uuid, utc_date, ts, energy, power, apparentpower)))
                        log.debug("executed prepared statements")

            else:
                log.debug(i + ' skipping.')
                # end for loop

                # time.sleep(0.1)
                # end while loop (1 sec / loop)
        log.info("finished writing to DB")

    prefix = int(argv[0])
    insertcount = int(argv[1])

    keyspace = "sense"

    log = logging.getLogger()
    log.setLevel('INFO')
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
    log.addHandler(handler)

    conn = httplib.HTTPConnection("192.168.0.105", 8080)
    cluster = Cluster(['128.32.189.230'], port=9042)
    session = cluster.connect()

    log.info("setting keyspace...")
    session.set_keyspace(keyspace)

    loop_count = 0

    pool = ThreadPool(processes=3)

    prepared = session.prepare("""
                            Insert into data (device_id, day, tp, actenergy, actpower, aparpower)
                            VALUES (?, ?, ?, ?, ?, ?)
                            """)

    log.debug("created prepared statements")
    #create utc datetime so we can remove time portion.
    utc = pytz.utc

    while True:
        if loop_count == 0:
            read1 = pool.apply_async(get_json, ())

        try:
            reading = read1.get(timeout=TIMEOUT)
        except TimeoutError:
            log.info("timeout reading, give up and continue.")

        if loop_count == 0:
            update_time = {m: 0 for m in reading['/costas_acmes']['Contents']}

        write1 = pool.apply_async(parse_write, ())
        read1 = pool.apply_async(get_json, ())

        try:
            done_writing = write1.get(timeout=TIMEOUT)
        except TimeoutError:
            log.info("timeout writing, give up and continue.")
        loop_count += 1

    conn.close()


if __name__ == "__main__":
    main(sys.argv[1:])
