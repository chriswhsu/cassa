__author__ = 'chriswhsu'
import uuid


class Device:
    def __init__(self, external_identifier, name, sw=None, deviceuuid=None, geohash=None, measures=None, tags=None,
                 parent_device_id=None):
        if deviceuuid is None:
            self.deviceuuid = uuid.uuid4()
        elif type(deviceuuid) == type('string'):
            self.deviceuuid = uuid.UUID(deviceuuid)
        else:
            self.deviceuuid = deviceuuid
        self.external_identifier = external_identifier
        self.name = name
        self.geohash = geohash
        self.measures = measures
        self.tags = tags
        self.parent_device_id = parent_device_id

        self.sns = sw

    def persist(self):

        return self.sns.reg_device(self)
