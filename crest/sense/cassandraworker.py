__author__ = 'chriswhsu'

import logging
import os
import ConfigParser
import datetime
import pytz

from cassandra.cluster import Cluster

from crest.sense.device import Device


class CassandraWorker(object):
    # a prepared statement to share across multiple writes
    prepared_statements = dict()
    registered_uuids = []
    external_id_to_dev_uuid = dict()

    # share prepared statments transparently by calling this instead of session.prepare.
    def prepare_shared(self, prepared):

        cql_hash = hash(prepared)

        if cql_hash in self.prepared_statements:
            pass
        else:
            self.prepared_statements[cql_hash] = self.session.prepare(prepared)

        return self.prepared_statements[cql_hash]


    def __init__(self, my_cluster=None, my_port=None, my_keyspace=None, test=True):
        config = ConfigParser.ConfigParser()
        config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'sense.cnf'))

        if my_cluster is None:
            my_cluster = config.get("Cassandra", "Cluster")
        if my_port is None:
            my_port = config.getint("Cassandra", "Port")

        self.log = logging.getLogger()
        self.log.setLevel(config.get("Logging", "Level"))
        self.handler = logging.StreamHandler()
        self.handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        self.log.addHandler(self.handler)

        self.cluster = Cluster([my_cluster], port=my_port)
        self.session = self.cluster.connect()

        if my_keyspace is None:
            if test:
                keyspace = config.get("Cassandra", "TestKeyspace")
                self.log.info("setting keyspace to %s" % keyspace)
                self.session.set_keyspace(keyspace)
            else:
                keyspace = config.get("Cassandra", "Keyspace")
                self.log.info("setting keyspace to %s" % keyspace)
                self.session.set_keyspace(keyspace)
        else:
            self.log.info("setting keyspace to %s" % my_keyspace)
            self.session.set_keyspace(my_keyspace)

    def register_measure(self, measure):
        self.log.debug('prepare statement')

        prepared = self.prepare_shared("""
          insert into measures (name, description, uom, datatype)
           values
          (?, ?, ?, ?)
         """)
        self.log.debug('execute prepared')

        self.session.execute(prepared.bind((
            measure.name, measure.description, measure.uom, measure.datatype)))
        self.log.debug('done with registering device')
        pass

    def get_all_measures(self):
        # get all unit of measures.
        pass


    def register_device(self, device):

        # enforce external_id uniqueness across entire device table.
        self.log.debug('start check for unique external_identifier')

        check = self.session.execute("select * from devices where external_identifier = %s",
                                     parameters=[device.external_identifier])
        self.log.debug('done with database hit')
        if len(check) == 1:
            if device.device_uuid == check[0].device_uuid:
                pass
            else:
                raise Exception("Device with the same external_identifier already exists")

        if len(check) > 1:
            raise Exception("we should never have more than one, please resolve data issue.")

        self.log.debug('prepare statement')

        prepared = self.prepare_shared("""
          insert into devices (device_uuid, geohash, name, external_identifier, measures, tags, parent_device_uuid,
                                latitude, longitude)
           values
          (?, ?, ?, ?, ?, ?, ?, ?, ?)
         """)
        self.log.debug('execute prepared')

        self.session.execute(prepared.bind((
            device.device_uuid, device.geohash, device.name, device.external_identifier,
            device.measures,
            device.tags,
            device.parent_device_uuid,
            device.latitude,
            device.longitude)))
        self.log.debug('done with registering device')
        return device.device_uuid


    def update_device(self, device):

        # clear cache in case external_identifier is changing:

        self.external_id_to_dev_uuid = dict()

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

        prepared = self.prepare_shared("""
          update devices set geohash = ?,
                             name = ?,
                             external_identifier = ?,
                             measures = ?,
                             tags = ?,
                             parent_device_uuid = ?,
                             latitude = ?,
                             longitude = ?
                where device_uuid = ?
         """)

        self.session.execute(prepared.bind((
            device.geohash, device.name, device.external_identifier,
            device.measures,
            device.tags,
            device.parent_device_uuid,
            device.latitude,
            device.longitude,
            device.device_uuid)))
        return device.device_uuid

    def get_all_devices(self):
        query = "SELECT * from devices"
        rows = self.session.execute(query)

        if len(rows) == 0:
            return None
        self.log.debug('We got %s rows' % len(rows))
        devices = []
        for row in rows:
            devices.append(Device(external_identifier=row.external_identifier, name=row.name,
                                  device_uuid=row.device_uuid, geohash=row.geohash, measures=row.measures,
                                  tags=row.tags, parent_device_uuid=row.parent_device_uuid, latitude=row.latitude,
                                  longitude=row.longitude))

        return devices


    def get_device(self, device_uuid):

        # should only return a single row.
        query = "SELECT * from devices where device_uuid = ?"
        prepared = self.prepare_shared(query)
        future = self.session.execute_async(prepared.bind([device_uuid]))

        rows = future.result()

        if len(rows) == 0:
            return None
        self.log.info('We got %s rows' % len(rows))
        # sets returned as sorted list, not simple list

        device = Device(external_identifier=rows[0].external_identifier, name=rows[0].name,
                        device_uuid=rows[0].device_uuid, geohash=rows[0].geohash, measures=rows[0].measures,
                        tags=rows[0].tags, parent_device_uuid=rows[0].parent_device_uuid, latitude=rows[0].latitude,
                        longitude=rows[0].longitude)

        return device


    def get_device_id_by_external_id(self, external_identifier):

        if external_identifier in self.external_id_to_dev_uuid:
            return self.external_id_to_dev_uuid[external_identifier]

        else:
            dev_uuid = self._get_device_id_by_external_id(external_identifier)

            if (dev_uuid):
                self.external_id_to_dev_uuid[external_identifier] = dev_uuid
                return dev_uuid
            else:
                return None


    def _get_device_id_by_external_id(self, external_identifier):
        query = "SELECT device_uuid from devices where external_identifier = ?"
        prepared = self.prepare_shared(query)

        future = self.session.execute_async(prepared.bind([external_identifier]))

        try:
            rows = future.result()
            self.log.info('We got %s rows' % len(rows))
            if len(rows) != 1:
                self.log.exception('We should have exactly 1 row, we got %s',len(rows))
                raise Exception

            return rows[0].device_uuid

        except Exception:

            self.log.exception('unhandled exception in _get_device_id_by_external_id')
            raise

    def get_device_ids_by_name(self, name):

        try:
            query = "SELECT device_uuid from devices where name = ?"
            prepared = self.prepare_shared(query)

            future = self.session.execute_async(prepared.bind([name]))
            rows = future.result()
            self.log.info('We got %s rows' % len(rows))
            devices = [row.device_uuid for row in rows]
            return devices
        except Exception:
            self.log.exeception('unhandled exception in get_device_ids_by_name')
            raise

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

    def get_device_uuids_by_measure(self, measure):
        """ Return device_uuids that are able to report certain measures."""
        #
        # Probably fast enough to simply iterate through all devices measures.
        # took 300 ms for 1,000 devices.
        devices = self.get_all_devices()

        match_dev = []

        for dev in devices:
            if (dev.measures):
                if measure in dev.measures:
                    match_dev.append(dev.device_uuid)

        return match_dev

    def get_device_uuids_by_tags(self):
        """ Return device_uuids that contain specific tags."""
        # TODO implement method get_device_uuids_by_tags
        pass

    def write_data_with_ext_id(self, external_id, timepoint, tuples):
        dev_uuid = self.get_device_id_by_external_id(external_id)
        self.write_data(dev_uuid, timepoint, tuples)


    def write_data(self, device_uuid, timepoint, tuples):
        self.log.debug("before column wrangling")

        # check in cache of registerd uuids
        if device_uuid in self.registered_uuids:
            pass
        else:
            # if not there, check the database
            device = self.get_device(device_uuid)
            if device is None:
                raise Exception("This device: %s does not exist, please register before writing data." % device_uuid)
            else:
                # it was there in DB, add to cache.
                self.registered_uuids.append(device.device_uuid)

        columns = ''
        col_q = ''
        values = []
        for x in tuples:
            columns += ', ' + x[0]
            col_q += ', ' + '?'
            values.append(x[1])

        prepared = self.prepare_shared(
            " Insert into data (device_uuid, day, tp" + columns + " ) values ( ?, ?, ?" + col_q + ")")

        #create utc datetime so we can remove time portion.
        self.log.debug("before pytz")
        utc = pytz.utc

        utc_datetime = datetime.datetime.fromtimestamp(timepoint, utc)
        utc_date = utc_datetime.replace(hour=0, minute=0, second=0, microsecond=0)
        self.log.debug("after time wrangling")
        self.session.execute(prepared.bind((device_uuid, utc_date, timepoint) + tuple(values)))
        self.log.debug("after write")

    def write_bulk_data(self, device_uuid, some_kind_of_an_array_of_kvps):
        # TODO figure out the best array structure
        pass

    def get_data_range(self, list_of_uuids, start_date, stop_date):
        rows = []
        days = (stop_date + datetime.timedelta(days=1) - start_date).days
        # TODO refactor to use parallel threads for multiple day retrieval.
        for uuid in list_of_uuids:
            for x in range(days):
                new_rows = self.read_data(uuid, start_date + datetime.timedelta(days=x))
                rows += new_rows
        return rows

    def read_data(self, uuid, utc_date):

        # get device details
        device = self.get_device(uuid)

        # create string of data types this device should contain
        columns = ''
        for x in device.measures:
            columns += x + ', '

        query = "SELECT " + columns + "tp FROM data where device_uuid = ? and day = ?"

        try:
            prepared = self.session.prepare(query)
            future = self.session.execute_async(prepared.bind([uuid, utc_date]))

            rows = future.result()
            self.log.info('We got %s rows' % len(rows))
        except Exception:
            self.log.exeception('unhandled exception in read_data')
            raise
        return rows

