from gevent import sleep
from collections import deque
from mrkt.framework.role.task import Task
from mrkt.framework.role import Cluster
from mrkt.common.consts import *


class Pool(Cluster):
    def __init__(self, *args, **kwargs):
        super(Pool, self).__init__(*args, **kwargs)
        self.task_queue = deque()

    def schedule(self):
        while True:
            for worker in self.workers:
                if self.need_sync(worker):
                    self.sync_worker(worker)
                else:
                    while worker.utilization() < 1:
                        if self.task_queue:
                            task = self.task_queue.popleft()
                            self.processing_tasks.append(task)
                            task.assign_to(worker)
                        else:
                            break
            sleep(CLUSTER_SCHEDULE_INTERVAL)

    def submit(self, func, *args, **kwargs):
        task = Task(func, args, kwargs)
        self.task_queue.append(task)
        return task

    def map(self, func, *iterables):
        tasks = [self.submit(func, *args) for args in zip(*iterables)]
        for task in tasks:
            task.join()
            yield task.ret
