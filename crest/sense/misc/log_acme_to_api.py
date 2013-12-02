#!/usr/bin/env python
TIMEOUT = 10  # seconds

__author__ = 'chriswhsu'
import httplib
import json
import sys
from multiprocessing.pool import ThreadPool, TimeoutError
from time import sleep

import crest.sense.sensefactory


def main(argv):
    def get_json():
        sns.log.debug('t-getting data with web request')
        conn = httplib.HTTPConnection("192.168.0.105", 8080)
        conn.request("GET", "/data/+")
        r = conn.getresponse()
        sns.log.debug('t-got response: ' + str(r.status) + ' ' + r.reason)
        dat_str = r.read()
        results = json.loads(dat_str)
        sns.log.info('finished get_json.')
        return results

    def parse_write():
        for i in reading['/costas_acmes']['Contents']:

            i = str(i)

            sns.log.debug("processing %s", i)

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
                    sns.log.info('i = %s, ts = %s, power = %s, energy = %s', i, ts, power, energy)
                    sns.write_data_with_ext_id(external_id=i, timepoint=ts, tuples=(
                        ('aparPower', apparentpower), ('actPower', power), ('actEnergy', energy) ))

            else:
                sns.log.debug(i + ' skipping.')
                # end for loop

                # time.sleep(0.1)
                # end while loop (1 sec / loop)
        sns.log.info("finished parse_write")

    sns = crest.sense.sensefactory.build_worker()
    sns.log.info('STARTING in main')

    pool = ThreadPool(processes=3)

    loop_count = 0
    r_timeouts = 0
    w_timeouts = 0

    update_time = dict()

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
            sns.log.info("Read: TimeoutError, give up and continue.")
            r_timeouts += 1
        except httplib.CannotSendRequest:
            sns.log.info("Read: CannotSendRequest, sleep and continue.")
            sleep(10)
            r_timeouts += 1
        except httplib.BadStatusLine:
            sns.log.info("Read: BadStatusLine, sleep and continue.")
            sleep(10)
            r_timeouts += 1

        if success:

            if loop_count == 0:
                for meter in reading['/costas_acmes']['Contents']:
                    # FILLING WITH 0's for first pass.
                    update_time[meter] = 0
                    ## Register, incase of new meter
                    #dev = crest.sense.sensefactory.build_device(name='Acme Meter: %s' % meter, external_identifier=meter,
                    #                                      measures={'aparPower', 'actPower', 'actEnergy'})
                    #sns.register_device(dev)

            write1 = pool.apply_async(parse_write, ())
            reader = pool.apply_async(get_json, ())

            try:
                done_writing = write1.get(timeout=TIMEOUT)
                w_timeouts = 0
                loop_count += 1
            except TimeoutError:
                sns.log.info("Write: TimeoutError, give up and continue.")
                w_timeouts += 1

            sleep(.5)


if __name__ == "__main__":
    main(sys.argv[1:])
