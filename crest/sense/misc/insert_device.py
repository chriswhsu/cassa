import uuid

from crest.sense.cassandraworker import Device, CassandraWorker


sns = CassandraWorker(my_cluster='sbb01.eecs.berkeley.edu', my_port=9042, my_keyspace='sense')

"""Test for successful persisting of a new device."""
device = Device(external_identifier='45ZZ71234', name="acme2",
                device_uuid=uuid.UUID('b17d661d-7e61-49ea-96a5-68c34e83db44'))
sns.register_device(device)




#
#from crest.sense.cassandraworker import Device, CassandraWorker
#
#sns = CassandraWorker(my_cluster='sbb01.eecs.berkeley.edu', my_port=9042, my_keyspace='sense')
#device_id = sns.get_device_ids_by_name('acme')
#device = sns.get_device(device_id[0])
#print device