__author__ = 'chriswhsu'

from crest.sense import cassandraworker
from crest.sense.device import Device



def build_worker():
    return cassandraworker.CassandraWorker(True)


def build_device(**kwargs):
    return Device(**kwargs)