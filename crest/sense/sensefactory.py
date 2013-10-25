__author__ = 'chriswhsu'

from crest.sense import senseworker
from crest.sense.device import Device



def build_worker():
    return senseworker.SenseWorker(True)


def build_device(**kwargs):
    return Device(**kwargs)