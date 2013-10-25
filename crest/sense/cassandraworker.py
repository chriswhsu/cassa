__author__ = 'chriswhsu'

import logging
import os
import ConfigParser
import datetime

from cassandra.cluster import Cluster

from crest.sense.device import Device


class CassandraWorker(object):
    def __init__(self, test=True):
        config = ConfigParser.ConfigParser()
        config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'sense.cnf'))

        self.log = logging.getLogger()
        self.log.setLevel(config.get("Logging", "Level"))
        self.handler = logging.StreamHandler()
        self.handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        self.log.addHandler(self.handler)

        self.cluster = Cluster([config.get("Cassandra", "Cluster")], port=config.getint("Cassandra", "Port"))
        self.session = self.cluster.connect()

        if test:
            config_get = config.get("Cassandra", "TestKeyspace")
            self.log.info("setting keyspace to %s" % config_get)
            self.session.set_keyspace(config_get)
        else:
            config_get = config.get("Cassandra", "Keyspace")
            self.log.info("setting keyspace to %s" % config_get)
            self.session.set_keyspace(config_get)

    def register_device(self, device):

        # enforce external_id uniqueness across entire device table.
        check = self.session.execute("select * from devices where external_identifier = %s",
                                     parameters=[device.external_identifier])
        if len(check) == 1:
            if device.device_uuid == check[0].device_uuid:
                pass
            else:
                raise Exception("Device with the same external_identifier already exists")

        if len(check) > 1:
            raise Exception("we should never have more than one, please resolve data issue.")

        prepared = self.session.prepare("""
          insert into devices (device_uuid, geohash, name, external_identifier, measures, tags, parent_device_id,
                                latitude, longitude)
           values
          (?, ?, ?, ?, ?, ?, ?, ?, ?)
         """)

        prepared.consistency_level = 5
        self.session.execute(prepared.bind((
            device.device_uuid, device.geohash, device.name, device.external_identifier,
            device.measures,
            device.tags,
            device.parent_device_id,
            device.latitude,
            device.longitude)))
        return device.device_uuid

    def get_device(self, device_uuid):

        query = ("SELECT * from devices where device_uuid = ?")
        prepared = self.session.prepare(query)
        prepared.consistency_level = 5
        future = self.session.execute_async(prepared.bind([device_uuid]))

        rows = future.result()
        self.log.info('We got %s rows' % len(rows))

        device = Device(external_identifier=rows[0].external_identifier, name=rows[0].name,
                        device_uuid=rows[0].device_uuid, geohash=rows[0].geohash, measures=rows[0].measures,
                        tags=rows[0].tags, parent_device_id=rows[0].parent_device_id, latitude=rows[0].latitude,
                        longitude=rows[0].longitude)

        return device


    def get_device_ids_by_external_id(self, external_identifier):
        query = ("SELECT device_uuid from devices where external_identifier = ?")
        prepared = self.session.prepare(query)

        future = self.session.execute_async(prepared.bind([external_identifier]))

        try:
            rows = future.result()
            self.log.info('We got %s rows' % len(rows))
            devices = [row.device_uuid for row in rows]
            return devices
        except Exception:
            self.log.exeception()

    def get_device_ids_by_name(self, name):

        try:
            query = "SELECT device_uuid from devices where name = ?"
            prepared = self.session.prepare(query)

            future = self.session.execute_async(prepared.bind([name]))
            rows = future.result()
            self.log.info('We got %s rows' % len(rows))
            devices = [row.device_uuid for row in rows]
            return devices
        except Exception:
            self.log.exeception()

    def get_device_ids_by_geohash(self, geohash_value, meters):
        """ get list of device_ids (UUID) based on distance from reference point
            use haversine for accurate location since we should be iterating over small list of devices
        """
        import geohash
        import haversine

        query = "SELECT device_uuid, geohash from devices"

        try:
            rows = self.session.execute(query)
            self.log.info('We got %s rows' % len(rows))
        except Exception:
            self.log.exception('failed query execution.')
            raise
        point = geohash.decode(geohash_value)



        # create list of devices whose distance is less than <meters> from geohash_value.
        # first decode geohash to lat / long
        # than calculated haversine distance between that point and target
        devices = [row.device_uuid for row in rows
                   if haversine.haversine(point, geohash.decode(row.geohash)) <= meters]
        return devices

    def get_device_uuids_by_measures(self):
        """ Return device_uuids that are able to report certain measures."""
        # TODO implment method get_device_uuids_by_measures
        pass

    def get_device_uuids_by_tags(self):
        """ Return device_uuids that contain specific tags."""
        # TODO implement method get_device_uuids_by_tags
        pass

    def write_data(self, device_uuid, timepoint, list_of_kvp):
        # TODO figure out how to best create api for writing data.
        prepared = self.session.prepare("""
                                    Insert into ddata (deviceID,day,timepoint,feeds,event)
                                    VALUES (?, ?, ?, ?, ?)
                                    """)

        self.log.info("created prepared statements")
        #create utc datetime so we can remove time portion.
        utc = pytz.utc
        utc_datetime = datetime.datetime.fromtimestamp(ts, utc)
        utc_date = utc_datetime.replace(hour=0, minute=0, second=0, microsecond=0)

        myValues = {'Power': Power, 'ApparentPower': ApparentPower, 'Energy': Energy}

        self.session.execute(prepared.bind((ID, utc_date, ts, myValues, '')))
        sleep(.5)


    def write_bulk_data(self, device_uuid, some_kind_of_an_array_of_kvps):
        # TODO figure out the best array structure
        pass


    def get_data_range(self, list_of_uuids, start_date, stop_date):
        rows = []
        days = (stop_date + datetime.timedelta(days=1) - start_date).days
        for uuid in list_of_uuids:
            for x in range(days):
                new_rows = self.get_data(uuid, start_date + datetime.timedelta(days=x))
                rows += new_rows
        return rows


    def get_data(self, uuid, utc_date):
        query = "SELECT actenergy, tp FROM data where device_uuid = ? and day = ?"

        try:
            prepared = self.session.prepare(query)
            future = self.session.execute_async(prepared.bind([uuid, utc_date]))

            rows = future.result()
            self.log.info('We got %s rows' % len(rows))
        except Exception:
            self.log.exeception()
        return rows

