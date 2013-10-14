#!/usr/bin/env python

__author__ = 'chriswhsu'
import httplib
import json
import datetime
import logging
import uuid
import sys
from time import sleep
import threading

import pytz
from cassandra.cluster import Cluster


def main(argv):
    threadnum = int(argv[0])
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
    utc = pytz.utc

    loop_count = 0

    def cass_write(session, prepared, dev_uuid, utc_date, ts, energy, power, apparentpower):
        session.execute(prepared.bind((dev_uuid, utc_date, ts, energy, power, apparentpower)))

    def get_json():

        log.info('------getting data with web request')
        conn.request("GET", "/data/+")
        r = conn.getresponse()
        log.info('got response: ' + str(r.status) + ' ' + r.reason)
        dat_str = r.read()
        reading = json.loads(dat_str)
        log.info('parsed into json.')
        return reading


    while True:

        reading = get_json()

        threads = []

        if loop_count == 0:
            update_time = {m: 0 for m in reading['/costas_acmes']['Contents']}

        loop_count += 1

        for i in reading['/costas_acmes']['Contents']:

            i = str(i)

            log.debug("processing %s in loop %d", i,loop_count)

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
                    id = i
                    ts = (int(t['ap'][reading_id]) / 1000)
                    power = v['tp'][reading_id]
                    apparentpower = v['ap'][reading_id]
                    energy = v['te'][reading_id]

                    prepared = session.prepare("""
                                                Insert into data (device_id, day, tp, actenergy, actpower, aparpower)
                                                VALUES (?, ?, ?, ?, ?, ?)
                                                """)

                    log.debug("created prepared statements")
                    #create utc datetime so we can remove time portion.

                    utc_datetime = datetime.datetime.fromtimestamp(ts, utc)
                    utc_date = utc_datetime.replace(hour=0, minute=0, second=0, microsecond=0)

                    prefix = 10000000 * threadnum

                    for p in range(prefix, prefix + insertcount + 1):
                        dev_uuid = uuid.UUID(str(p) + '-0000-0000-0000-000000000' + i)

                        trd = threading.Thread(target=cass_write, args=(
                            session, prepared, dev_uuid, utc_date, ts, energy, power, apparentpower))
                        threads.append(trd)
                        trd.start()


            else:
                log.debug(i + ' skipping.')
                # end for loop

                # time.sleep(0.1)
                # end while loop (1 sec / loop)

        log.info("Num threads: %d" % len(threads))
        for t_count in range(len(threads)):
            threads[t_count].join()
            log.debug("%d thread joined" % t_count)

        log.info("All threads completed.")

    conn.close()


if __name__ == "__main__":
    main(sys.argv[1:])