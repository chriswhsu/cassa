__author__ = 'chriswhsu'

from crest.sense.senseworker import SenseWorker

sns = SenseWorker(test=False)

# sns.session.execute("""drop table if exists devices""")
# sns.log.info('dropped table devices.')

sns.session.execute("""CREATE TABLE spacetime (
                         geohash text,
                         day datetime,
                         device_uuid uuid,
                         PRIMARY KEY (geohash,day)
                       ) WITH
                         compression={'sstable_compression': 'SnappyCompressor'}""")

sns.session.execute("""CREATE INDEX external_id_ind ON devices (external_identifier)""")
sns.session.execute("""CREATE INDEX name_ind ON devices (name)""")
sns.session.execute("""CREATE INDEX geohash_ind ON devices (geohash)""")
sns.session.execute("""CREATE INDEX parent_device_id_ind ON devices (parent_device_id)""")

sns.log.info('created table devices.')

# only create the most specific record  upon initial tp-value insert.
# have batch process create demormailzied records at lessor degrees of hash specificity.


# inserted at same time as data table for records with geohash values.
# How precise to make the index?
# aabbc 2013-10-10 --> AB BD CD
# aabb  2013-10-10 --> AB BD CD DD
# aab   2013-10-10 --> AB BD CD DD EE FF JJ
# aa    2013-10-10 --> AB BD CD DD EE FF JJ
# a     2013-10-10 --> AB BD CD DD EE FF JJ HH II JJ KK LL MM NN


# GeoDevices x Days

# 1 Year --> 1,000 devices * 365 days * 8 geohash specificities = 3 million records.
#           (compared to 90 billion data records)

# nope, what if the device moves?  duh.  We may have index entries per time point.

