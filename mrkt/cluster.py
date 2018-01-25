from mrkt.agent import Worker
from .service import BaseService
from .platform.base import BasePlatform
from .utils import flatten_iterables, run_on_each

import gevent

class Cluster:
    def __init__(self, services, **options):
        self.services = [s for s in services if isinstance(s, BaseService)]
        self.platforms = [s for s in services if isinstance(s, BasePlatform)]
        servers_on_platforms = run_on_each(self.platforms, "services")
        self.services.extend(flatten_iterables(*servers_on_platforms))
        run_on_each(self.services, "update_options", async=False, options=options)
        self.workers = []

    def __enter__(self):
        self.prepare()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.clean()

    def prepare(self):
        run_on_each(self.services, "prepare")
        self.start_workers()

    def clean(self):
        run_on_each(self.services, "clean")
        run_on_each(self.services, "close")
        run_on_each(self.platforms, "clean")

    def start_workers(self):
        if self.workers:
            run_on_each(self.services, "stop_workers")
        self.workers = flatten_iterables(
            *run_on_each(self.services, "start_workers"))

    def sync_dir(self, path, remote_path=None):
        remote_path = remote_path or path
        delta = self.workers[0].sync_dir_delta(path, remote_path)
        run_on_each(self.workers, "sync_dir_patch", delta=delta, remote_path=remote_path)

    def submit(self, func, *args, **kwargs):
        for worker in self.workers:
            if worker.utilization() < 1:
                return worker.async_exec(func, *args, **kwargs)
        return None

    def joinall(self, tasks):
        for task in tasks:
            task.join()

    def map(self, func, *iterables):
        args_list = list(zip(*iterables))
        tasks =[]

        def _schedule():
            for worker in self.workers:
                # print([w.tasks for w in self.workers])
                while worker.utilization() < 1:
                    if not args_list: return
                    args = args_list.pop(0)
                    task = worker.make_task(func)
                    tasks.append(task)
                    task.start(*args)
                    gevent.spawn(_wait, task)

        def _wait(task):
            task.join()
            _schedule()

        _schedule()
        self.joinall(tasks)
        return [task.ret for task in tasks]
