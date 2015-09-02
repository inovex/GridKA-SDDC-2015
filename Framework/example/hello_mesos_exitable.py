#!/usr/bin/env python
import sys
import logging
import uuid
import time
import signal
from mesos.interface import Scheduler, mesos_pb2
from mesos.native import MesosSchedulerDriver

logging.basicConfig(level=logging.INFO)


def shutdown(signal, frame):
    logging.info("Shutdown signal")
    driver.stop()
    sys.exit(0)


def new_task(offer):
    task = mesos_pb2.TaskInfo()
    task.task_id.value = str(uuid.uuid4())
    task.slave_id.value = offer.slave_id.value
    task.name = "HelloWorld"

    cpus = task.resources.add()
    cpus.name = "cpus"
    cpus.type = mesos_pb2.Value.SCALAR
    cpus.scalar.value = 0.1

    mem = task.resources.add()
    mem.name = "mem"
    mem.type = mesos_pb2.Value.SCALAR
    mem.scalar.value = 10

    return task


class HelloWorldScheduler(Scheduler):
    def registered(self, driver, framework_id, master_info):
        logging.info("Registered with framework id: %s on: %s",
             framework_id, master_info.hostname)

    def resourceOffers(self, driver, offers):
        logging.info("Recieved resource offers: %s",
                     [o.id.value for o in offers])
        # whenever we get an offer
        # we accept it and use it to launch a task that
        # just echos hello world to stdout
        for offer in offers:
            task = new_task(offer)
            task.command.value = "while [ true ]; do echo hello world && sleep 1 ; done"
            time.sleep(5)
            logging.info("Launching task %s "
                         "using offer %s.",
                         task.task_id.value,
                         offer.id.value)
            driver.launchTasks(offer.id, [task])

    def statusUpdate(self, driver, update):
        '''
        when a task is started, over,
        killed or lost (slave crash, ....), this method
        will be triggered with a status message.
        '''
        logging.info("Task %s is in state %s" %
                     (update.task_id.value,
                      mesos_pb2.TaskState.Name(update.state)))

if __name__ == '__main__':
    framework = mesos_pb2.FrameworkInfo()
    framework.user = ""  # Have Mesos fill in the current user.
    framework.name = "hello-world"
    driver = MesosSchedulerDriver(
        HelloWorldScheduler(),
        framework,
        "zk://localhost:2181/mesos"  # assumes running on the master
    )
    driver.start()
    logging.info("Listening for Ctrl-C")
    signal.signal(signal.SIGINT, shutdown)
    while True:
        time.sleep(5)
    sys.exit(0)
