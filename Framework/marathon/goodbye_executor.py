import threading
import sys
import time
from mesos.interface import mesos_pb2, Executor
from mesos.native import MesosExecutorDriver


class GoodByeExecutor(Executor):
    def registered(self, driver, executorInfo, frameworkInfo, slaveInfo):
        print "GoodByeExecutor registered"

    def reregistered(self, driver, slaveInfo):
        print "GoodByeExecutor reregistered"

    def disconnected(self, driver):
        print "GoodByeExecutor disconnected"

    def launchTask(self, driver, task):
        def run_task():
            def task_update(state):
                update = mesos_pb2.TaskStatus()
                update.task_id.value = task.task_id.value
                update.state = state
                driver.sendStatusUpdate(update)
            print "Running GoodBye task %s" % task.task_id.value
            task_update(mesos_pb2.TASK_RUNNING)

            for i in range(0, 100):
                print "%s says Good Bye" % task.task_id.value
                time.sleep(2)

            print "Sending status update for task %s" % task.task_id.value
            task_update(mesos_pb2.TASK_FINISHED)
            print "Sent status update for task %s" % task.task_id.value
            return
        threading.Thread(target=run_task).start()

if __name__ == "__main__":
    print "Starting GoodBye Executor"
    driver = MesosExecutorDriver(GoodByeExecutor())
    sys.exit(0 if driver.run() == mesos_pb2.DRIVER_STOPPED else 1)
