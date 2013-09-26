__author__ = 'chriswhsu'
import httplib
import json
import time
import datetime
import logging
from time import sleep

import pytz

from cassandra.cluster import Cluster


KEYSPACE = "wherewhenwhat"

log = logging.getLogger()
log.setLevel('DEBUG')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

meters = ['936']
update_time = {m: 0 for m in meters}

conn = httplib.HTTPConnection("192.168.0.105", 8080)

cluster = Cluster(['128.32.189.129'], port=7902)
session = cluster.connect()

log.info("setting keyspace...")
session.set_keyspace(KEYSPACE)

while True:
    print datetime.datetime.now()
    conn.request("GET", "/data/+")
    r = conn.getresponse()
    print r.status, r.reason
    dat_str = r.read()
    reading = json.loads(dat_str)

    for i in meters:
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

                print(
                "ID=%s, Time=%s, Power=%s, AparrentPower=%s, Energy=%s" % (ID, ts, Power, ApparentPower, Energy))

                prepared = session.prepare("""
                                            Insert into ddata (deviceID,day,timepoint,feeds,event)
                                            VALUES (?, ?, ?, ?, ?)
                                            """)

                log.info("created prepared statements")
                #create utc datetime so we can remove time portion.
                utc = pytz.utc
                utc_datetime = datetime.datetime.fromtimestamp(ts,utc)
                utc_date = utc_datetime.replace(hour=0, minute=0, second=0, microsecond=0)

                myValues = {'Power': Power, 'ApparentPower': ApparentPower, 'Energy': Energy}

                session.execute(prepared.bind((ID, utc_date, ts, myValues, '')))
                sleep(.5)

        else:
            print i, '---'
        # end for loop

    time.sleep(0.8)
    # end while loop (1 sec / loop)

conn.close()