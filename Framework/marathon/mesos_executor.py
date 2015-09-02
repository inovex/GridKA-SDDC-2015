#!/usr/bin/env python
import sys
import logging
import threading
import uuid
import signal
import time
import datetime

from mesos.interface import Scheduler, mesos_pb2
from mesos.native import MesosSchedulerDriver

logging.basicConfig(level=logging.INFO)
TASK_CPUS = 0.1
TASK_MEM = 256
SHUTDOWN_TIMEOUT = 15


def new_task(offer, name):
    task = mesos_pb2.TaskInfo()
    task.task_id.value = str(uuid.uuid4())
    task.slave_id.value = offer.slave_id.value
    task.name = name

    cpus = task.resources.add()
    cpus.name = "cpus"
    cpus.type = mesos_pb2.Value.SCALAR
    cpus.scalar.value = TASK_CPUS

    mem = task.resources.add()
    mem.name = "mem"
    mem.type = mesos_pb2.Value.SCALAR
    mem.scalar.value = TASK_MEM

    return task


def max_tasks_to_run_with_offer(offer):
    logging.info("CPUs: %s MEM: %s",
                 offer.resources[0].scalar.value,
                 offer.resources[1].scalar.value)

    cpu_tasks = int(offer.resources[0].scalar.value / TASK_CPUS)
    mem_tasks = int(offer.resources[1].scalar.value / TASK_MEM)

    return cpu_tasks if cpu_tasks <= mem_tasks else mem_tasks


class HelloWorldScheduler(Scheduler):
    def __init__(self, hello_executor, goodbye_executor):
        self.runningTasks = 0
        self.hello_executor = hello_executor
        self.goodbye_executor = goodbye_executor
        self.shuttingDown = False

    def registered(self, driver, framework_id, master_info):
        logging.info("Registered with framework id: %s on: %s",
                     framework_id, master_info.hostname)

    def resourceOffers(self, driver, offers):
        logging.info("Recieved resource offers: %s",
                     [o.id.value for o in offers])
        for offer in offers:
            def handle_offer():
                count_tasks = max_tasks_to_run_with_offer(offer)
                logging.info("Count Tasks: %s", count_tasks)
                if count_tasks == 0:
                    logging.info("Decline Offer %s", offer.id)
                    driver.declineOffer(offer.id)
                    return

                tasks = []
                for i in range(count_tasks):
                    if i % 2 == 0:
                        task = new_task(offer, "Hello ")
                        task.executor.MergeFrom(self.hello_executor)
                    else:
                        task = new_task(offer, "GoodBye ")
                        task.executor.MergeFrom(self.goodbye_executor)
                    logging.info("Added task %s "
                                 "using offer %s.",
                                 task.task_id.value,
                                 offer.id.value)
                    tasks.append(task)
                logging.info("Launch %s Tasks", len(tasks))
                driver.launchTasks(offer.id, tasks)

            threading.Thread(target=handle_offer).start()

    def statusUpdate(self, driver, update):
        '''
        when a task is started, over,
        killed or lost (slave crash, ....), this method
        will be triggered with a status message.
        '''
        logging.info("Task %s is in state %s" %
                     (update.task_id.value,
                      mesos_pb2.TaskState.Name(update.state)))

        if update.state == mesos_pb2.TASK_RUNNING:
            self.runningTasks += 1
            logging.info("Running tasks: %s", self.runningTasks)
            return

        if update.state != mesos_pb2.TASK_RUNNING or \
                        update.state != mesos_pb2.TASK_STARTING or \
                        update.state != mesos_pb2.TASK_STAGING:
            self.runningTasks -= 1
            logging.info("Running tasks: %s", self.runningTasks)


def graceful_shutdown(signal, frame):
    print "HelloWorldScheduler is shutting down"
    helloWorldScheduler.shuttingDown = True

    wait_started = datetime.datetime.now()
    while (helloWorldScheduler.runningTasks > 0) and \
            (SHUTDOWN_TIMEOUT > (datetime.datetime.now() - wait_started).total_seconds()):
        time.sleep(1)

    if helloWorldScheduler.runningTasks > 0:
        print "Shutdown by timeout, %d task(s) have not completed" % helloWorldScheduler.runningTasks

    hard_shutdown()


def hard_shutdown():
    logging.info("Hard shutdown signal")
    driver.stop()
    sys.exit(0)


if __name__ == '__main__':
    hello_executor = mesos_pb2.ExecutorInfo()
    hello_executor.executor_id.value = "hello-executor"
    hello_executor.name = "Hello"
    hello_executor.command.value = "python hello_executor.py"

    uri_proto = hello_executor.command.uris.add()
    uri_proto.value = "http://kit-mesos-master:9000/hello_executor.py"
    uri_proto.extract = False

    goodbye_executor = mesos_pb2.ExecutorInfo()
    goodbye_executor.executor_id.value = "goodbye-executor"
    goodbye_executor.name = "GoodBye"
    goodbye_executor.command.value = "python goodbye_executor.py"

    uri_proto = goodbye_executor.command.uris.add()
    uri_proto.value = "http://kit-mesos-master:9000/goodbye_executor.py"
    uri_proto.extract = False

    framework = mesos_pb2.FrameworkInfo()
    framework.user = ""  # Have Mesos fill in the current user.
    framework.name = "hello-world"

    helloWorldScheduler = HelloWorldScheduler(hello_executor, goodbye_executor)

    driver = MesosSchedulerDriver(
        helloWorldScheduler,
        framework,
        "kit-mesos-master:5050"
    )
    driver.start()
    logging.info("Listening for Ctrl-C")
    signal.signal(signal.SIGINT, graceful_shutdown)
    while True:
        time.sleep(5)
    sys.exit(0)
