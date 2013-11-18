__author__ = 'chriswhsu'
import uuid
import geohash as gh


class Device:

    external_identifier = None
    name = None
    device_uuid = None
    geohash = None
    measures = None
    tags = None
    parent_device_id = None
    latitude = None
    longitude = None

    def __init__(self, external_identifier, name, device_uuid=None, geohash=None, measures=None, tags=None,
                 parent_device_id=None, latitude=None, longitude=None):
        """ initialize new device object
        """


        # prevent inconsistant geospatial data from making it into repository
        if geohash and (latitude or longitude):
            raise Exception("don't populate both geohash and lat / long")

        # but populate both to facilitate queries that need lat/long data.
        if (latitude and longitude) and not geohash:
            geohash = gh.encode(latitude, longitude)

        if geohash and not (latitude or longitude):
            (latitude, longitude) = gh.decode(geohash)

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
        return 'device_uuid: ' + str(
            self.device_uuid) + ', name: ' + self.name + ', external_identifier: ' + self.external_identifier