__author__ = 'chriswhsu'

from sense import cassandraworker
from sense.device import Device


def build_worker(test=True):
    return cassandraworker.CassandraWorker(test=test)


def build_device(**kwargs):
    return Device(**kwargs)