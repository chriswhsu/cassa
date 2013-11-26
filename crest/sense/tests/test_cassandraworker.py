__author__ = 'chriswhsu'

import unittest
import uuid
import time
from crest.sense.measure import Measure
from crest.sense.device import Device
from crest.sense.cassandraworker import CassandraWorker


class TestCassandraWorker(unittest.TestCase):
    # Create CassandraWorker to maintain a single database connection throughout tests.

    sns = CassandraWorker(test=True)

    def tearDown(self):
        self.sns.log.info("tearDown: truncating devices table.")
        self.sns.session.execute('truncate devices')
        self.sns.registered_uuids = []
        self.sns.log.info("tearDown: DONE truncating devices table.")

        self.sns.log.info("tearDown: truncating data table.")
        self.sns.session.execute('truncate data')
        self.sns.registered_uuids = []
        self.sns.log.info("tearDown: DONE truncating data table.")

    def test_measure_creation(self):
        """Test for successful instantiation and ersisting of device"""

        measure = Measure(name='Temp', description='Temperature in Centigrade', uom='Degrees C', datatype='Float')

        self.sns.register_measure(measure)
        # just want to make it here without abort.

    def test_device_creation(self):
        """Test for successful persisting of a new device."""
        device = Device(external_identifier='tdc', name="tdc_name",
                        device_uuid=uuid.uuid4())
        self.sns.register_device(device)
        # just want to make it here without abort.

    def test_single_external_id(self):
        """Test retrieval of device by external_identifier"""
        uuid_ = uuid.uuid4()
        device = Device(external_identifier='testSingle', name='testDevice2',
                        device_uuid=uuid_)
        self.sns.register_device(device)
        device_uuid = self.sns.get_device_id_by_external_id('testSingle')
        self.assertEqual(device_uuid, uuid_)

    def test_device_with_measures(self):
        """Test creating a device with associated measures"""
        device = Device(external_identifier='testSingle', name='testDevice2',
                        measures={'volts', 'total_watts'})
        self.sns.register_device(device)
        device_uuid = self.sns.get_device_id_by_external_id('testSingle')

        device = self.sns.get_device(device_uuid)
        self.assertListEqual(sorted(device.measures), sorted([u'volts', u'total_watts']))

    def test_get_device_by_measure(self):
        """Test retrieval of device by external_identifier"""
        device = Device(external_identifier='testSingle', name='testDevice2',
                        measures={'volts', 'total_watts'})
        the_device = self.sns.register_device(device)
        self.sns.log.info('start device creation')
        for x in range(2):
            device2 = Device(external_identifier='tdc_%s' % x, name="tdc_name_%s" % x, measures={'temp', 'humidity'})
            self.sns.register_device(device2)
            self.sns.log.debug('Done with device: %s', x)
        self.sns.log.info('done with device creation, try retrieval')

        device_uuids = self.sns.get_device_uuids_by_measure('volts')

        self.assertIn(the_device, device_uuids)
        self.sns.log.info('done with retrieval')

    def test_novel_uuid_device_creation(self):
        """Test creating novel device without specified UUID"""
        device = Device(external_identifier='tdc', name="tdc_name")
        result = self.sns.register_device(device)
        self.assertTrue(isinstance(result, uuid.UUID))

    def test_string_uuid(self):
        device = Device(external_identifier='tdp', name="tdp_name",
                        device_uuid='117d661d-7e61-49ea-96a5-68c34e83db55')
        result = self.sns.register_device(device)

    def test_get_device_by_name(self):
        """Test retrieval of device by name"""
        uuid_ = uuid.uuid4()
        device = Device(external_identifier='testSingle', name='testDevice2',
                        device_uuid=uuid_)
        self.sns.register_device(device)
        device_uuids = self.sns.get_device_ids_by_name('testDevice2')
        self.assertIn(uuid_, device_uuids)

    def test_multiple_names(self):
        """Test creating multiple devices with different external_identifier, but same name and retrieving"""

        device = Device(external_identifier='test123', name='testDevice1')
        uuid1_ = self.sns.register_device(device)

        device = Device(external_identifier='test1234', name='testDevice1')
        uuid2_ = self.sns.register_device(device)

        devices = self.sns.get_device_ids_by_name('testDevice1')
        self.assertEqual(sorted(devices), sorted([uuid1_, uuid2_]))

    def test_prevent_dup_external_id(self):
        device = Device(external_identifier='test123', name='testDevice1')
        self.sns.register_device(device)

        # will get a new UUID when device object is instantiated
        device = Device(external_identifier='test123', name='testDevice1')

        with self.assertRaises(Exception):
            self.sns.register_device(device)

    def test_allow_same_device_id(self):

        """ Same external identifier allowed if device_uuid is the same.
            In which case it's really an update. """

        uuid_ = uuid.uuid4()

        device = Device(external_identifier='test123', name='testDevice1', device_uuid=uuid_)
        self.sns.register_device(device)

        device = Device(external_identifier='test123', name='welcome', device_uuid=uuid_)
        updated = self.sns.register_device(device)

        updated_device = self.sns.get_device(updated)
        self.assertEqual(updated_device.name, 'welcome')

    def test_update_device(self):

        uuid_ = uuid.uuid4()

        device = Device(external_identifier='test123', name='testDevice1', device_uuid=uuid_)
        self.sns.register_device(device)

        device = Device(external_identifier='test123', name='welcome', device_uuid=uuid_)
        updated = self.sns.update_device(device)

        updated_device = self.sns.get_device(updated)

        self.assertEqual(updated_device.name, 'welcome')

    def test_geohash(self):
        """Test retrieval by distance from geohash"""

        target_device = self.sns.register_device(
            Device(external_identifier='geo:1', name='geohash1', geohash='gcpvhep'))

        self.sns.register_device(Device(external_identifier='geo:2', name='geohash2', latitude=51.5177893638,
                                        longitude=-0.1417708396911))
        self.sns.register_device(Device(external_identifier='geo:3', name='geohash2', geohash='gcpvhfb'))

        self.sns.register_device(Device(external_identifier='geo:4', name='geohash2', geohash='gcpvhfr'))

        # There should be 3 points within 0.5 meters
        devices = self.sns.get_device_ids_by_geohash('gcpvhep', .5)
        self.assertEqual(len(devices), 3)

        # Only one at this exact point
        devices = self.sns.get_device_ids_by_geohash('gcpvhep', 0)
        self.assertEqual(len(devices), 1)
        self.assertEqual(devices, [target_device])

    def test_get_device(self):
        device1 = Device(external_identifier='tdc', name="tdc_name")
        device1_id = self.sns.register_device(device1)

        device2 = self.sns.get_device(device1_id)
        # not the same object, just different objects representing the same row.
        self.sns.log.info(device1)
        self.sns.log.info(device2)
        self.assertNotEquals(device1, device2)
        # all attributes inside should be the same.
        self.assertEqual(device1.device_uuid, device2.device_uuid)

    def test_get_nonexistent_device(self):

        device = self.sns.get_device(uuid.uuid4())

        self.sns.log.info(device)
        self.assertIsNone(device)

    def test_ensure_device_registered(self):
        self.sns.log.info('start write.')
        with self.assertRaises(Exception): self.sns.write_data(
            device_uuid=uuid.uuid4(), timepoint=time.time(),
            tuples=(('temp', 35.6),))

        self.sns.log.info('finish write.')

    def test_write_data_one_feed(self):
        device = Device(external_identifier='twdof', name="twdof_name")
        device_uuid = self.sns.register_device(device)
        self.sns.log.info('start write.')
        self.sns.write_data(device_uuid=device_uuid, timepoint=time.time(),
                            tuples=(('temp', 35.6),))
        self.sns.log.info('finish write.')
        # just hoping to get this far without an exception
        self.assertEquals(1, 1)

    def test_write_data_multiple_feeds(self):
        device = Device(external_identifier='twdof', name="twdof_name")
        device_uuid = self.sns.register_device(device)
        self.sns.log.info('start write.')
        self.sns.write_data(device_uuid, time.time(),
                            (('temp', 35.6), ('humidity', 99), ('solar_rad', 625), ('wind_dir', 23.5)))
        self.sns.log.info('finish write.')
        # just hoping to get this far without an exception
        self.assertEquals(1, 1)

    def test_write_data_100_record_feed(self):
        device = Device(external_identifier='twdof', name="twdof_name")
        device_uuid = self.sns.register_device(device)
        self.sns.log.info('start big write.')
        for x in range(100):
            self.sns.write_data(device_uuid, time.time(),
                                (('temp', 35.6), ('humidity', 99), ('solar_rad', 625), ('wind_dir', 23.5)))
        self.sns.log.info('finish big write.')
        # just hoping to get this far without an exception
        self.assertEquals(1, 1)

    def test_write_data_invalid_feed(self):
        import time

        device = Device(external_identifier='twdof', name="twdof_name")
        device_uuid = self.sns.register_device(device)
        with self.assertRaises(Exception): self.sns.write_data(device_uuid,
                                                               time.time(),
                                                               (('baloney_price', 23.5),))

    def test_read_data(self):
        from crest.sense.utility import gmt_date

        device = Device(external_identifier='tgd', name="tgd_name",
                        measures=['temp', 'humidity', 'solar_rad', 'wind_dir'])
        d_uuid = self.sns.register_device(device)
        self.sns.log.info('start write.')

        for x in range(10):
            ts = time.time()
            self.sns.write_data(d_uuid, time.time(),
                                (('temp', 35.6), ('humidity', 99), ('solar_rad', 625), ('wind_dir', 23.5)))

        rows = self.sns.read_data(d_uuid, gmt_date())

        for row in rows:
            self.sns.log.info(row)

        self.assertEquals(len(rows), 10)
