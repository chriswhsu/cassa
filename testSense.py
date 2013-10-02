__author__ = 'chriswhsu'

import unittest
import uuid

from crest import senseWorker
from crest import Device


class TestDevice(unittest.TestCase):
    # Create SenseWorker to maintain a single database connection throughout tests.

    sns = senseWorker.SenseWorker()

    def setUp(self):
        self.sns.log.info("setUp: truncating devices table.")
        self.sns.session.execute('truncate devices')

    def test_device_creation(self):
        """Test for successful persisting of a new device."""
        device = Device.Device(sw=self.sns, deviceuuid=uuid.UUID('e17d661d-7e61-49ea-96a5-68c34e83db44'),
                               external_identifier='tdc', name="tdc_name")
        device.persist()

    def test_single_external_id(self):
        """Test retrieval of device by external_identifier"""
        device = Device.Device(sw=self.sns, deviceuuid=uuid.UUID('c17d661d-7e61-49ea-96a5-68c34e83db55'),
                               external_identifier='testSingle', name='testDevice2')
        device.persist()
        devices = self.sns.getdevices_by_external_id('testSingle')
        self.assertEqual(devices, [uuid.UUID('c17d661d-7e61-49ea-96a5-68c34e83db55')])

    def test_multiple_external_ids(self):
        """Test creating multiple devices with same external_identifier and retrieving"""
        device = Device.Device(sw=self.sns, deviceuuid=uuid.UUID('b17d661d-7e61-49ea-96a5-68c34e83db44'),
                               external_identifier='test123', name='testDevice1')
        device.persist()

        device = Device.Device(sw=self.sns, deviceuuid=uuid.UUID('a17d661d-7e61-49ea-96a5-68c34e83db33'),
                               external_identifier='test123', name='testDevice2')
        device.persist()

        devices = self.sns.getdevices_by_external_id('test123')
        self.assertEqual(devices, [uuid.UUID('b17d661d-7e61-49ea-96a5-68c34e83db44'),
                                   uuid.UUID('a17d661d-7e61-49ea-96a5-68c34e83db33')])

    def test_novel_uuid_device_creation(self):
        """Test creating novel device without specified UUID"""
        device = Device.Device(sw=self.sns, external_identifier='tdc', name="tdc_name")
        result = device.persist()
        self.assertTrue(isinstance(result, uuid.UUID))

    def test_string_uuid(self):
        device = Device.Device(sw=self.sns, deviceuuid='117d661d-7e61-49ea-96a5-68c34e83db55',
                               external_identifier='tdp', name="tdp_name")
        result = device.persist()

    def test_single_name(self):
        """Test retrieval of device by external_identifier"""
        device = Device.Device(sw=self.sns, deviceuuid=uuid.UUID('c17d661d-7e61-49ea-96a5-68c34e83db55'),
                               external_identifier='testSingle', name='testDevice2')
        device.persist()
        devices = self.sns.getdevices_by_name('testDevice2')
        self.assertEqual(devices, [uuid.UUID('c17d661d-7e61-49ea-96a5-68c34e83db55')])

    def test_geohash(self):
        """Test retrieval by distance from geohash"""

        Device.Device(sw=self.sns, external_identifier='geo1', name='geohash1', geohash='gcpvhep').persist()
        Device.Device(sw=self.sns, external_identifier='geo2', name='geohash2', geohash='gcpvhf8').persist()
        Device.Device(sw=self.sns, external_identifier='geo2', name='geohash2', geohash='gcpvhfb').persist()

        Device.Device(sw=self.sns, external_identifier='geo2', name='geohash2', geohash='gcpvhfr').persist()

        devices = self.sns.getdevices_by_geohash('gcpvhep', 5)


class TestSenseWorker(unittest.TestCase):
    def test_connection(self):
        sns = senseWorker.SenseWorker(test=True)
        sns.log.info("create senseWorker")




        #rows = sns.getdatarange(devices, datetime.datetime(2013, 9, 27, 0, 0, 0, 0), datetime.datetime(2013, 9, 27, 0, 0, 0, 0))

        #for row in rows:
        #    print row