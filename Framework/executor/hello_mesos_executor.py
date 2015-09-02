#!/usr/bin/env python
import sys
import logging
import threading
import uuid
import signal
import time

from mesos.interface import Scheduler, mesos_pb2
from mesos.native import MesosSchedulerDriver

logging.basicConfig(level=logging.INFO)
TASK_CPUS = 0.1
TASK_MEM = 256


def new_task(offer):
    task = mesos_pb2.TaskInfo()
    task.task_id.value = str(uuid.uuid4())
    task.slave_id.value = offer.slave_id.value
    task.name = "HelloWorld"

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

    cpu_tasks = int(offer.resources[0].scalar.value/TASK_CPUS)
    mem_tasks = int(offer.resources[1].scalar.value/TASK_MEM)

    return cpu_tasks if cpu_tasks <= mem_tasks else mem_tasks


class HelloWorldScheduler(Scheduler):
    def __init__(self, hello_executor):
        self.runningTasks = 0
        self.hello_executor = hello_executor

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
                for i in range(count_tasks / 2):
                    task = new_task(offer)
                    task.executor.MergeFrom(self.hello_executor)
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


def shutdown(signal, frame):
    logging.info("Shutdown signal")
    driver.stop()
    sys.exit(0)

if __name__ == '__main__':
    framework = mesos_pb2.FrameworkInfo()
    framework.user = ""  # Have Mesos fill in the current user.
    framework.name = "hello-world"

    helloWorldExecutor = mesos_pb2.ExecutorInfo()
    helloWorldExecutor.executor_id.value = "HelloWorld-executor"
    helloWorldExecutor.command.value = "while [ true ]; do echo hello world && sleep 1 ; done"
    helloWorldExecutor.name = "HelloWorld"

    helloWorldScheduler = HelloWorldScheduler(helloWorldExecutor)
    driver = MesosSchedulerDriver(
        helloWorldScheduler,
        framework,
        "zk://localhost:2181/mesos"  # assumes running on the master
    )

    driver.start()
    logging.info("Listening for Ctrl-C")
    signal.signal(signal.SIGINT, shutdown)
    while True:
        time.sleep(5)
    sys.exit(0)
