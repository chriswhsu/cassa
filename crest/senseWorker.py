__author__ = 'chriswhsu'

import logging
import os
import ConfigParser
import datetime
import uuid

from cassandra.cluster import Cluster


class SenseWorker(object):
    def __init__(self, test=False):
        config = ConfigParser.ConfigParser()
        config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'crest.cnf'))

        self.log = logging.getLogger()
        self.log.setLevel('DEBUG')
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        self.log.addHandler(handler)

        self.cluster = Cluster([config.get("Cassandra", "Cluster")], port=config.getint("Cassandra", "Port"))
        self.session = self.cluster.connect()

        if test:
            config_get = config.get("Cassandra", "TestKeyspace")
            self.log.info("setting keyspace to %s"%config_get)
            self.session.set_keyspace(config_get)
        else:
            config_get = config.get("Cassandra", "Keyspace")
            self.log.info("setting keyspace to %s"%config_get)
            self.session.set_keyspace(config_get)

    def reg_device(self, device):
        prepared = self.session.prepare("""
          insert into devices (device_id, geohash, name, external_identifier, measures, tags, parent_device_id)
           values
          (?, ?, ?, ?, ?, ?, ?)
         """)
        self.session.execute(prepared.bind((
            device.deviceuuid, device.geohash, device.name, device.external_identifier,
            device.measures,
            device.tags,
            device.parent_device_id)))
        return device.deviceuuid


    def getdevice(self, external_identifier):
        global devices
        query = ("SELECT device_id from devices where external_identifier = ?")
        prepared = self.session.prepare(query)

        future = self.session.execute_async(prepared.bind([external_identifier]))

        try:
            rows = future.result()
            self.log.info ('We got %s rows' % len(rows))
            devices = [row.device_id for row in rows]
        except Exception:
            self.log.exeception()
        return devices

    def getdatarange(self, list_of_uuids, start_date, stop_date):
        rows = []
        days = (stop_date + datetime.timedelta(days=1) - start_date).days
        for uuid in list_of_uuids:
            for x in range(days):
                new_rows = self.getdata(uuid, start_date + datetime.timedelta(days=x))
                rows += new_rows
        return rows;

    def getdata(self, uuid, utc_date):
        query = ("SELECT actenergy, tp FROM data where device_id = ? and day = ?")
        prepared = self.session.prepare(query)

        future = self.session.execute_async(prepared.bind([uuid, utc_date]))

        try:
            rows = future.result()
            self.log.info ('We got %s rows' % len(rows))
        except Exception:
            self.log.exeception()
        return rows

