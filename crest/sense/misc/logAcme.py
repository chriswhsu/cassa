#!/usr/bin/env python

__author__ = 'chriswhsu'
import httplib
import json
import datetime
import logging
import uuid

import pytz

from cassandra.cluster import Cluster


KEYSPACE = "test"

log = logging.getLogger()
log.setLevel('INFO')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

conn = httplib.HTTPConnection("192.168.0.105", 8080)

cluster = Cluster(['128.32.189.230'], port=9042)
session = cluster.connect()

log.info("setting keyspace...")
session.set_keyspace(KEYSPACE)

loop_count = 0

while True:
    log.info('getting data with web request')
    conn.request("GET", "/data/+")
    r = conn.getresponse()
    log.info('got response: ' + str(r.status) + ' ' + r.reason)
    dat_str = r.read()
    reading = json.loads(dat_str)
    log.info('parsed into json.')

    if loop_count ==0:
        update_time = {m: 0 for m in reading['/costas_acmes']['Contents']}

    loop_count += 1



    for i in reading['/costas_acmes']['Contents']:

        i = str(i)

        log.info("processing %s", i)

        v = {'ap': [False, False], 'tp': [False, False], 'te': [False, False]}
        t = {'ap': [False, False], 'tp': [False, False], 'te': [False, False]}

        index1 = '/costas_acmes/' + i + '/apparent_power'
        index2 = '/costas_acmes/' + i + '/true_power'
        index3 = '/costas_acmes/' + i + '/true_energy'

        if reading.has_key(index1):
            t['ap'][0] = reading[index1]['Readings'][0][0]
            v['ap'][0] = reading[index1]['Readings'][0][1]
            t['ap'][1] = reading[index1]['Readings'][1][0]
            v['ap'][1] = reading[index1]['Readings'][1][1]

        if reading.has_key(index2):
            t['tp'][0] = reading[index2]['Readings'][0][0]
            v['tp'][0] = reading[index2]['Readings'][0][1]
            t['tp'][1] = reading[index2]['Readings'][1][0]
            v['tp'][1] = reading[index2]['Readings'][1][1]

        if reading.has_key(index3):
            t['te'][0] = reading[index3]['Readings'][0][0]
            v['te'][0] = reading[index3]['Readings'][0][1]
            t['te'][1] = reading[index3]['Readings'][1][0]
            v['te'][1] = reading[index3]['Readings'][1][1]

        if t['ap'][1] > update_time[i]:
            # log into file
            #filename = 'log_'+i+'.csv'
            #f=open(filename,'a')
            #line1=str(t['ap'][0]) +','+ str(v['ap'][0]) +','+ str(v['tp'][0]) +','+ str(v['te'][0]) +'\n'
            #line2=str(t['ap'][1]) +','+ str(v['ap'][1]) +','+ str(v['tp'][1]) +','+ str(v['te'][1]) +'\n'
            #f.write(line1)
            #f.write(line2)
            #f.close()
            #print i,t['ap'],t['tp'],t['te'],v['ap'],v['tp'],v['te']
            update_time[i] = t['ap'][1]
            # update to database
            for reading_id in range(0, 2):
                ID = i
                ts = (int(t['ap'][reading_id]) / 1000)
                Power = v['tp'][reading_id]
                ApparentPower = v['ap'][reading_id]
                Energy = v['te'][reading_id]

                log.info(
                    "ID=%s, Time=%s, Power=%s, AparrentPower=%s, Energy=%s" % (ID, ts, Power, ApparentPower, Energy))

                prepared = session.prepare("""
                                            Insert into data (device_id, day, tp, actenergy, actpower, aparpower)
                                            VALUES (?, ?, ?, ?, ?, ?)
                                            """)

                log.debug("created prepared statements")
                #create utc datetime so we can remove time portion.
                utc = pytz.utc
                utc_datetime = datetime.datetime.fromtimestamp(ts, utc)
                utc_date = utc_datetime.replace(hour=0, minute=0, second=0, microsecond=0)

                for p in range(40000000,40000009):
                    dev_uuid = uuid.UUID(str(p) + '-58cc-4372-a567-0e02b2c3d' + i)
                    session.execute(prepared.bind((dev_uuid, utc_date, ts, Energy, Power, ApparentPower)))
                    log.debug("executed prepared statements")

        else:
            log.info(i + ' skipping.')
            # end for loop

            # time.sleep(0.1)
            # end while loop (1 sec / loop)

conn.close()