import sys
sys.path.append("/Users/chriswhsu/PycharmProjects/cassa")
from locust import Locust, TaskSet, task, events
from crest.sense.device import Device
from crest.sense import cassandraworker
import time
import uuid

class UserBehavior(TaskSet):

    def on_start(self):
        """ on_start is called when a Locust start before any task is scheduled """


    def login(self):
        pass

    @task(2)
    def toprint(self):
        device = Device(external_identifier=str(time.time()), name="tdc_name",
                        device_uuid=uuid.UUID('e17d661d-7e61-49ea-96a5-68c34e83db44'))

        sns = cassandraworker.CassandraWorker(test=True)
        sns.register_device(device)
        events.request_success.fire("MethodA","NameA",10,100)

    @task(1)
    def toprint2(self):
        print("test2")
        events.request_success.fire("MethodB","NameB",1,10)

class WebsiteUser(Locust):
    task_set = UserBehavior
    min_wait=5000
    max_wait=9000