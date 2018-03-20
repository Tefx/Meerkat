from ...common import patch;

patch()
import gevent
from gevent.pool import Group

from ...common import call_on_each
from ...common.consts import *


class SyncStack:
    class SyncLayer:
        def __init__(self, path):
            self.path = path
            self.delta = None

    def __init__(self):
        self.layers = []
        self.latest_tag = 0
        self.sync_group = Group()

    def append(self, path):
        self.layers.append(self.SyncLayer(path))

    def has_unknown_delta(self):
        return self.latest_tag < len(self.layers)

    def need_sync(self, worker):
        return worker.sync_tag < self.latest_tag

    def update_delta(self, worker):
        assert self.has_unknown_delta()
        assert worker.sync_tag == self.latest_tag
        layer = self.layers[self.latest_tag]
        layer.delta = worker.calc_dir_delta(layer.path)
        self.latest_tag += 1

    def start_sync(self, worker):
        def _sync():
            while worker.sync_tag < self.latest_tag:
                layer = self.layers[worker.sync_tag]
                worker.sync_with_delta(layer.delta, layer.path)
            worker.set_syncing(False)

        worker.set_syncing(True)
        self.sync_group.spawn(_sync)

    def stop(self):
        self.sync_group.kill()


class Cluster:
    def submit(self, func, *args, **kwargs):
        raise NotImplementedError

    def schedule(self):
        raise NotImplementedError

    def __init__(self, platforms, sync_current_dir=CLUSTER_SYNC_CURRENT_DIR, **options):
        self.platforms = platforms
        self.processing_tasks = []
        self.sync_manager = SyncStack()
        call_on_each(self.platforms, "prepare_services", options=options)
        self.scheduler = gevent.spawn(self.schedule)
        if sync_current_dir:
            self.sync_dir(".")

    def clean(self):
        self.scheduler.kill()
        self.sync_manager.stop()
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
                self.sync_manager.start_sync(worker)
            elif self.sync_manager.has_unknown_delta():
                self.sync_manager.update_delta(worker)
                self.sync_manager.start_sync(worker)
