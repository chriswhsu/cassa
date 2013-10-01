__author__ = 'chriswhsu'
import uuid


class Device:


    def __init__(self, external_identifier, name, sw, deviceuuid=None, geohash=None, measures=None, tags=None,
                 parent_device_id=None):
        """ initialize new device object """
        self.external_identifier = external_identifier
        self.name = name
        self.geohash = geohash
        self.measures = measures
        self.tags = tags
        self.parent_device_id = parent_device_id

        if deviceuuid is None:
            self.deviceuuid = uuid.uuid4()
        elif isinstance(deviceuuid, basestring):
            self.deviceuuid = uuid.UUID(deviceuuid)
        else:
            self.deviceuuid = deviceuuid
        # we pass in a senseWorker to maintain single connection.
        # used below in persist.
        self.sns = sw

    def persist(self):
        """ persist new device object """
        return self.sns.reg_device(self)
