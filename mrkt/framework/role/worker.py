import os.path
from logging import getLogger
from gevent.lock import BoundedSemaphore

from ...agent import DynamicAgent
from ...common.utils import dir_delta
from .task import Task

logger = getLogger(__name__)
null_agent = DynamicAgent()


class Worker:
    def __init__(self, agent_addr, parallel_task_limit=None):
        self.agent_addr = agent_addr
        self.tasks = set()
        self.ptask_semaphore = None
        self.capacity = parallel_task_limit or self.cpu_count()
        self.ptask_semaphore = BoundedSemaphore(self.capacity)
        self.sync_tag = 0
        self.sync_flag = False

    def utilization(self):
        return len([t for t in self.tasks if not t.is_adm_task()]) / self.capacity

    def is_available(self):
        return self.utilization() < 1

    def wait_until_idle(self):
        if self.ptask_semaphore is not None:
            self.ptask_semaphore.acquire()

    def on_finish_task(self, task):
        self.tasks.remove(task)
        task.clean()
        if self.ptask_semaphore is not None:
            self.ptask_semaphore.release()

    def __getattr__(self, name):
        index = "_adm_{}".format(name)
        func = getattr(null_agent, index)

        def adm_func(*args, **kwargs):
            task = Task(func, args, kwargs, index)
            task.assign_to(self)
            task.join()
            return task.ret

        return adm_func

    def __repr__(self):
        return "Client[{}]".format(self.agent_addr)

    def calc_dir_delta(self, path):
        sig = self.dir_signature(path, os.path.isdir(path))
        logger.info("[Worker.calc_delta]: Got signature[size:%s] from %s", len(sig), self.agent_addr[0])
        delta = dir_delta(sig, path)
        logger.info("[Worker.calc_delta]: Delta calculated[size:%s]", len(delta))
        return delta

    def sync_with_delta(self, delta, path):
        self.dir_patch(delta, path)
        logger.info("[Worker.sync] with %s: Patch finished", self.agent_addr[0])
        self.clean_cache()
        logger.info("[Worker.sync] with %s: Cache cleaned", self.agent_addr[0])
        self.sync_tag += 1

    def is_syncing(self):
        return self.sync_flag

    def set_syncing(self, value):
        self.sync_flag = value

    def clean(self):
        for task in self.tasks:
            task.kill()
        self.tasks = set()
