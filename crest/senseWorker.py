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
        self.handler = logging.StreamHandler()
        self.handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        self.log.addHandler(self.handler)

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


    def getdevices_by_external_id(self, external_identifier):
        query = ("SELECT device_id from devices where external_identifier = ?")
        prepared = self.session.prepare(query)

        future = self.session.execute_async(prepared.bind([external_identifier]))

        try:
            rows = future.result()
            self.log.info ('We got %s rows' % len(rows))
            devices = [row.device_id for row in rows]
            return devices
        except Exception:
            self.log.exeception()

    def getdevices_by_name(self, name):
        query = "SELECT device_id from devices where name = ?"
        prepared = self.session.prepare(query)

        future = self.session.execute_async(prepared.bind([name]))

        try:
            rows = future.result()
            self.log.info ('We got %s rows' % len(rows))
            devices = [row.device_id for row in rows]
            return devices
        except Exception:
            self.log.exeception()

    def getdevices_by_geohash(self, geohash_value, meters):
        import geohash
        import haversine

        query = "SELECT device_id, geohash from devices"

        try:
            rows = self.session.execute(query)
            self.log.info ('We got %s rows' % len(rows))
        except Exception:
            self.log.exception('failed query execution.')
            raise
        target_point = geohash.decode(geohash_value)

        # create list of devices whose distance is less than <meters> from geohash_value.
        # first decode geohash to lat / long
        # than calculated haversine distance between that point and target_point.
        devices = [(row.device_id) for row in rows
                   if haversine.haversine(target_point, geohash.decode(row.geohash)) <= meters ]
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
        query = "SELECT actenergy, tp FROM data where device_id = ? and day = ?"
        prepared = self.session.prepare(query)

        future = self.session.execute_async(prepared.bind([uuid, utc_date]))

        try:
            rows = future.result()
            self.log.info ('We got %s rows' % len(rows))
        except Exception:
            self.log.exeception()
        return rows

