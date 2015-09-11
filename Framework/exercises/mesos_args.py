#!/usr/bin/env python
import argparse
import sys
import logging
import uuid
import signal
from mesos.interface import Scheduler, mesos_pb2
from mesos.native import MesosSchedulerDriver
import time

logging.basicConfig(level=logging.INFO)
RUNNING_TASKS = 5


def new_task(offer, name, cmd, cpu, memory):
    task = mesos_pb2.TaskInfo()
    task.task_id.value = str(uuid.uuid4())
    task.slave_id.value = offer.slave_id.value
    task.name = name
    task.command.value = cmd

    cpus = task.resources.add()
    cpus.name = "cpus"
    cpus.type = mesos_pb2.Value.SCALAR
    cpus.scalar.value = cpu

    mem = task.resources.add()
    mem.name = "mem"
    mem.type = mesos_pb2.Value.SCALAR
    mem.scalar.value = memory

    return task


def new_docker_task(offer, name, cmd, cpu, memory, image):
    task = new_task(offer, name, cmd, cpu, memory)

    container = mesos_pb2.ContainerInfo()
    container.type = 1

    docker = mesos_pb2.ContainerInfo.DockerInfo()
    docker.image = image
    docker.network = 3
    docker.force_pull_image = True

    container.docker.MergeFrom(docker)
    task.container.MergeFrom(container)

    return task


def max_tasks_to_run_with_offer(offer, cpu, mem):
    logging.info("CPUs: %s MEM: %s",
                 offer.resources[0].scalar.value,
                 offer.resources[1].scalar.value)

    cpu_tasks = int(offer.resources[0].scalar.value / cpu)
    mem_tasks = int(offer.resources[1].scalar.value / mem)

    return cpu_tasks if cpu_tasks <= mem_tasks else mem_tasks


def shutdown(signal, frame):
    logging.info("Shutdown signal")
    driver.stop()
    time.sleep(5)
    sys.exit(0)


class HelloWorldScheduler(Scheduler):
    def __init__(self, cmd, run_docker, mem, cpu, image):
        self.runningTasks = 0
        self.cmd = cmd
        self.run_docker = run_docker
        self.cpu = cpu
        self.mem = mem
        self.image = image

    def registered(self, driver, framework_id, master_info):
        logging.info("Registered with framework id: %s on: %s",
                     framework_id, master_info.hostname)

    def resourceOffers(self, driver, offers):
        logging.info("Recieved resource offers: %s",
                     [o.id.value for o in offers])
        tasks_to_start = RUNNING_TASKS - self.runningTasks
        for offer in offers:
            if RUNNING_TASKS <= self.runningTasks:
                driver.declineOffer(offer.id)
                return
            count_tasks = max_tasks_to_run_with_offer(offer, self.cpu, self.mem)
            start_tasks = count_tasks if count_tasks <= tasks_to_start else tasks_to_start
            tasks_to_start -= start_tasks

            if start_tasks <= 0:
                logging.info("Decline Offer %s", offer.id)
                driver.declineOffer(offer.id)
                return

            logging.info("Start %s tasks", start_tasks)
            tasks = []
            for i in range(start_tasks):
                task = new_docker_task(offer,
                                       "Hello Docker",
                                       self.cmd,
                                       self.cpu,
                                       self.mem,
                                       self.image) \
                    if self.run_docker \
                    else new_task(offer,
                                  "Hello Task",
                                  self.cmd,
                                  self.cpu,
                                  self.mem)

                logging.info("Added task %s "
                             "using offer %s.",
                             task.task_id.value,
                             offer.id.value)
                tasks.append(task)
            logging.info("Launch %s Tasks", len(tasks))
            driver.launchTasks(offer.id, tasks)

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


def shutdown(signal, frame):
    logging.info("Shutdown signal")
    driver.stop()
    time.sleep(5)
    sys.exit(0)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='A simple Mesos Framework')
    parser.add_argument('--cmd', type=str, default='echo Hello World',
                        help='Command to run with the framework')
    parser.add_argument('--docker', type=bool, default=False,
                        help='If we should run Docker as containerizer')
    parser.add_argument('--mem', type=float, default=256.0,
                        help='How much memory is assigned to a task')
    parser.add_argument('--cpu', type=float, default=0.1,
                        help='How much memory is assigned to a task')
    parser.add_argument('--image', type=str, default="busybox",
                        help='Docker image to run')
    args = parser.parse_args()

    framework = mesos_pb2.FrameworkInfo()
    framework.user = ""
    framework.name = "hello-world"
    helloWorldScheduler = HelloWorldScheduler(args.cmd, args.docker,
                                              args.mem, args.cpu,
                                              args.image)
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
