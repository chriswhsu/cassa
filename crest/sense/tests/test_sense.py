from crest.sense.device import Device
from crest.sense import senseworker

__author__ = 'chriswhsu'

import unittest
import uuid


class TestDevice(unittest.TestCase):
    def test_create_device(self):
        device = Device(external_identifier='tdc', name="tdc_name", geohash='gcpp33',
                        device_uuid=uuid.UUID('e17d661d-7e61-49ea-96a5-68c34e83db44'))
        # don't have a logger object so just print.
        print(device)

        self.assertTrue(isinstance(device, Device))

    def test_prevent_inconsitant_geospatial(self):
        # If we try to populat both geohash and lat / long we expect an exception.
        with self.assertRaises(Exception): Device(external_identifier='geo2', name='geohash2', geohash='gcpp',
                                                  latitude=51,
                                                  longitude=-0.14)


class TestSenseWorker(unittest.TestCase):
    # Create SenseWorker to maintain a single database connection throughout tests.

    sns = senseworker.SenseWorker()

    def tearDown(self):
        self.sns.log.info("setUp: truncating devices table.")
        self.sns.session.execute('truncate devices')

    def test_device_creation(self):
        """Test for successful persisting of a new device."""
        device = Device(external_identifier='tdc', name="tdc_name",
                        device_uuid=uuid.UUID('e17d661d-7e61-49ea-96a5-68c34e83db44'))
        self.sns.register_device(device)

    def test_single_external_id(self):
        """Test retrieval of device by external_identifier"""
        device = Device(external_identifier='testSingle', name='testDevice2',
                        device_uuid=uuid.UUID('c17d661d-7e61-49ea-96a5-68c34e83db55'))
        self.sns.register_device(device)
        devices = self.sns.get_device_ids_by_external_id('testSingle')
        self.assertEqual(devices, [uuid.UUID('c17d661d-7e61-49ea-96a5-68c34e83db55')])

    def test_novel_uuid_device_creation(self):
        """Test creating novel device without specified UUID"""
        device = Device(external_identifier='tdc', name="tdc_name")
        result = self.sns.register_device(device)
        self.assertTrue(isinstance(result, uuid.UUID))

    def test_string_uuid(self):
        device = Device(external_identifier='tdp', name="tdp_name",
                        device_uuid='117d661d-7e61-49ea-96a5-68c34e83db55')
        result = self.sns.register_device(device)

    def test_dupliate_name(self):
        """Test retrieval of device by name"""
        device = Device(external_identifier='testSingle', name='testDevice2',
                        device_uuid=uuid.UUID('c17d661d-7e61-49ea-96a5-68c34e83db55'))
        self.sns.register_device(device)
        devices = self.sns.get_device_ids_by_name('testDevice2')
        self.assertEqual(devices, [uuid.UUID('c17d661d-7e61-49ea-96a5-68c34e83db55')])

    def test_multiple_names(self):
        """Test creating multiple devices with same external_identifier and retrieving"""
        device = Device(external_identifier='test123', name='testDevice1',
                        device_uuid=uuid.UUID('b17d661d-7e61-49ea-96a5-68c34e83db44'))
        self.sns.register_device(device)

        device = Device(external_identifier='test1234', name='testDevice1',
                        device_uuid=uuid.UUID('a17d661d-7e61-49ea-96a5-68c34e83db33'))
        self.sns.register_device(device)

        devices = self.sns.get_device_ids_by_name('testDevice1')
        self.assertEqual(devices, [uuid.UUID('b17d661d-7e61-49ea-96a5-68c34e83db44'),
                                   uuid.UUID('a17d661d-7e61-49ea-96a5-68c34e83db33')])

    def test_prevent_dup_external_id(self):
        device = Device(external_identifier='test123', name='testDevice1',
                        device_uuid=uuid.UUID('b17d661d-7e61-49ea-96a5-68c34e83db44'))
        self.sns.register_device(device)

        device = Device(external_identifier='test123', name='testDevice1',
                        device_uuid=uuid.UUID('a17d661d-7e61-49ea-96a5-68c34e83db33'))

        with self.assertRaises(Exception): self.sns.register_device(device)


    def test_allow_same_device_id(self):
        """ Same external identifier allowed if device_uuid is the same."""
        device = Device(external_identifier='test123', name='testDevice1',
                        device_uuid=uuid.UUID('b17d661d-7e61-49ea-96a5-68c34e83db44'))
        self.sns.register_device(device)

        device = Device(external_identifier='test123', name='testDevice10',
                        device_uuid=uuid.UUID('b17d661d-7e61-49ea-96a5-68c34e83db44'))
        updated = self.sns.register_device(device)

        updated_device = self.sns.get_device(updated)

        self.assertEqual(updated_device.name, 'testDevice10')

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
        device1 = Device(external_identifier='tdc', name="tdc_name",
                         device_uuid=uuid.UUID('e17d661d-7e61-49ea-96a5-68c34e83db44'))
        device1_id = self.sns.register_device(device1)

        device2 = self.sns.get_device(device1_id)
        # not the same object, just different objects representing the same row.
        self.sns.log.info(device1)
        self.sns.log.info(device2)
        self.assertNotEquals(device1, device2)
        # all attributes inside should be the same.
        self.assertEqual(device1.device_uuid, device2.device_uuid)

        #rows = sns.getdatarange(devices, datetime.datetime(2013, 9, 27, 0, 0, 0, 0), datetime.datetime(2013, 9, 27, 0, 0, 0, 0))

        #for row in rows:
        #    print row