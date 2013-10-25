from crest.sense.sensefactory import build_device

__author__ = 'chriswhsu'

import unittest
import uuid


class TestDevice(unittest.TestCase):
    def test_create_device(self):
        import crest.sense.device

        device = build_device(external_identifier='tdc', name="tdc_name", geohash='gcpp33',
                              device_uuid=uuid.UUID('e17d661d-7e61-49ea-96a5-68c34e83db44'))
        # don't have a logger object so just print.
        print(device)

        self.assertTrue(isinstance(device, crest.sense.device.Device))

    def test_prevent_inconsitant_geospatial(self):
        # If we try to populat both geohash and lat / long we expect an exception.
        with self.assertRaises(Exception): build_device(external_identifier='geo2', name='geohash2', geohash='gcpp',
                                                        latitude=51,
                                                        longitude=-0.14)

