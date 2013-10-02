__author__ = 'chriswhsu'
import uuid


class Device:

    def __init__(self, external_identifier, name, device_uuid=None, geohash=None, measures=None, tags=None,
                 parent_device_id=None, latitude=None, longitude=None):
        """ initialize new device object
        """
        self.external_identifier = external_identifier
        self.name = name
        self.geohash = geohash
        self.measures = measures
        self.tags = tags
        self.parent_device_id = parent_device_id
        self.latitude = latitude
        self.longitude = longitude

        if device_uuid is None:
            self.device_uuid = uuid.uuid4()
        elif isinstance(device_uuid, basestring):
            self.device_uuid = uuid.UUID(device_uuid)
        else:
            self.device_uuid = device_uuid


    def __str__(self):
        return 'deviceuuid: ' + str(self.device_uuid) + ', name: ' + self.name