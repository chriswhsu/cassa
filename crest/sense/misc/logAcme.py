#!/usr/bin/env python
TIMEOUT = 10  # seconds

__author__ = 'chriswhsu'
import httplib
import json
import datetime
import logging
import uuid
import sys
import os
from multiprocessing.pool import ThreadPool, TimeoutError
from time import sleep

import pytz
from cassandra.cluster import Cluster, NoHostAvailable

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
    conn = httplib.HTTPConnection("192.168.0.105", 8080)
    cluster = Cluster(['128.32.189.228','128.32.189.229','128.32.189.230'], port=9042)
    session = cluster.connect()
    session.set_keyspace(keyspace)

    prepared = session.prepare("""
                            Insert into data (device_id, day, tp, actenergy, actpower, aparpower)
                            VALUES (?, ?, ?, ?, ?, ?)
                            """)
    log.info('finished get_connections')
    return conn, cluster, session, prepared


def main(argv):
    def get_json():
        log.debug('t-getting data with web request')
        conn.request("GET", "/data/+")
        r = conn.getresponse()
        log.debug('t-got response: ' + str(r.status) + ' ' + r.reason)
        dat_str = r.read()
        results = json.loads(dat_str)
        log.info('finished get_json.')
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

                    for p in range(fullprefix, fullprefix + insertcount):
                        dev_uuid = uuid.UUID(str(p) + '-0000-0000-0000-000000000' + i)
                        session.execute(prepared.bind((dev_uuid, utc_date, ts, energy, power, apparentpower)))
                        log.debug("executed prepared statements")

            else:
                log.debug(i + ' skipping.')
                # end for loop

                # time.sleep(0.1)
                # end while loop (1 sec / loop)
        log.info("finished parse_write")

    log.info('STARTING in main')

    prefix = int(argv[0])
    insertcount = int(argv[1])

    pool = ThreadPool(processes=3)

    mycon = pool.apply_async(get_connections)
    try:
        conn, cluster, session, prepared = mycon.get(TIMEOUT)
    except TimeoutError:
        log.info('timeout creation connections.')
        raise

    except NoHostAvailable:
        log.info('no host available.')
        sleep(10)
        raise

    loop_count = 0
    r_timeouts = 0
    w_timeouts = 0

    while True:

        success = False

        # if we have 3 or more consecutive read or write timeouts
        # then re-establish all connections.
        if r_timeouts >= 3 or w_timeouts >= 3:
            raise TimeoutError
        if loop_count == 0:
            reader = pool.apply_async(get_json, ())
        try:
            reading = reader.get(timeout=TIMEOUT)
            r_timeouts = 0
            success = True
        except TimeoutError:
            log.info("Read: TimeoutError, give up and continue.")
            r_timeouts += 1
        except httplib.CannotSendRequest:
            log.info("Read: CannotSendRequest, sleep and continue.")
            sleep(10)
            r_timeouts += 1
        except httplib.BadStatusLine:
            log.info("Read: BadStatusLine, sleep and continue.")
            sleep(10)
            r_timeouts += 1

        if success:

            if loop_count == 0:
                update_time = {m: 0 for m in reading['/costas_acmes']['Contents']}

            write1 = pool.apply_async(parse_write, ())
            reader = pool.apply_async(get_json, ())

            try:
                done_writing = write1.get(timeout=TIMEOUT)
                w_timeouts = 0
                loop_count += 1
            except TimeoutError:
                log.info("Write: TimeoutError, give up and continue.")
                w_timeouts += 1

            sleep(.5)


if __name__ == "__main__":
    main(sys.argv[1:])
