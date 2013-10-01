__author__ = 'chriswhsu'

import unittest
import uuid

from crest import senseWorker
from crest import Device


class DeviceRetrieval(unittest.TestCase):
    sns = senseWorker.SenseWorker()

    def test_multiple_external_ids(self):
        device = self.sns.register_device(deviceuuid=uuid.UUID('b17d661d-7e61-49ea-96a5-68c34e83db44'),
                                          external_identifier='test123', name='testDevice1')

        device = self.sns.register_device(deviceuuid=uuid.UUID('a17d661d-7e61-49ea-96a5-68c34e83db33'),
                                          external_identifier='test123', name='testDevice2')

        devices = self.sns.getdevice('test123')
        self.assertEqual(devices, [uuid.UUID('b17d661d-7e61-49ea-96a5-68c34e83db44'),
                                   uuid.UUID('a17d661d-7e61-49ea-96a5-68c34e83db33')])

    def test_single_external_id(self):
        device = self.sns.register_device(deviceuuid=uuid.UUID('c17d661d-7e61-49ea-96a5-68c34e83db55'),
                                          external_identifier='testSingle', name='testDevice2')
        devices = self.sns.getdevice('testSingle')
        self.assertEqual(devices, [uuid.UUID('c17d661d-7e61-49ea-96a5-68c34e83db55')])

    def test_device_creation(self):
        device = Device.Device(deviceuuid=uuid.UUID('e17d661d-7e61-49ea-96a5-68c34e83db44'),
                               external_identifier='tdc', name="tdc_name")
        self.sns.reg_device(device)

    def test_novel_uuid_device_creation(self):
        device = Device.Device(external_identifier='tdc', name="tdc_name")
        self.sns.reg_device(device)

    def test_string_uuid(self):
        device = Device.Device(deviceuuid='117d661d-7e61-49ea-96a5-68c34e83db55', external_identifier='tdp', name="tdp_name")
        print device.persist()
        #rows = sns.getdatarange(devices, datetime.datetime(2013, 9, 27, 0, 0, 0, 0), datetime.datetime(2013, 9, 27, 0, 0, 0, 0))

        #for row in rows:
        #    print row