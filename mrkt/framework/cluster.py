import gevent
from collections import deque

from .task import Task
from ..common.consts import *
from ..common.utils import call_on_each


class SyncStack:
    class Layer:
        def __init__(self, path):
            self.path = path
            self.delta = None

    def __init__(self):
        self.layers = []
        self.latest_tag = 0

    def append(self, path):
        self.layers.append(self.Layer(path))

    def has_unknown_delta(self):
        return self.latest_tag < len(self.layers)

    def need_sync(self, worker):
        return worker.sync_tag < self.latest_tag

    def update_delta(self, worker):
        assert self.has_unknown_delta()
        assert worker.sync_tag == self.latest_tag
        layer = self.layers[self.latest_tag]
        layer.delta = worker.calculate_dir_delta(layer.path)
        self.latest_tag += 1

    def sync_worker(self, worker):
        while worker.sync_tag < self.latest_tag:
            layer = self.layers[worker.sync_tag]
            worker.sync_with_delta(layer.delta, layer.path)
        worker.set_syncing(False)

    def start_sync_worker(self, worker):
        worker.set_syncing(True)
        gevent.spawn(self.sync_worker, worker)


class Cluster:
    def __init__(self, platforms, sync_current_dir=CLUSTER_SYNC_CURRENT_DIR_DEFAULT, **options):
        self.platforms = platforms
        self.task_queue = deque()
        self.processing_tasks = []
        self.scheduler = gevent.spawn(self.schedule)
        self.sync_manager = SyncStack()
        call_on_each(self.platforms, "prepare_services", options=options)
        if sync_current_dir:
            self.sync_dir(".")

    def clean(self):
        self.scheduler.kill()
        call_on_each(self.platforms, "clean", join=True)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.clean()

    @property
    def workers(self):
        for platform in self.platforms:
            for service in platform.services:
                for worker in service.workers:
                    yield worker

    def sync_dir(self, path):
        self.sync_manager.append(path)

    def need_sync(self, worker):
        return worker.is_syncing() or self.sync_manager.need_sync(worker) or self.sync_manager.has_unknown_delta()

    def sync_worker(self, worker):
        if not worker.is_syncing():
            if self.sync_manager.need_sync(worker):
                self.sync_manager.start_sync_worker(worker)
            elif self.sync_manager.has_unknown_delta():
                self.sync_manager.update_delta(worker)
                self.sync_manager.start_sync_worker(worker)

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
            gevent.sleep(CLUSTER_SCHEDULE_INTERVAL)

    def submit(self, func, *args, **kwargs):
        task = Task(func, args, kwargs)
        self.task_queue.append(task)
        return task

    def map(self, func, *iterables):
        tasks = [self.submit(func, *args) for args in zip(*iterables)]
        for task in tasks:
            task.join()
            yield task.ret
