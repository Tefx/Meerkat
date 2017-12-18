from .agent import Client
from .service import BaseService
from .platform.base import BasePlatform
from .utils import flatten_iterables, run_on_each


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
        run_on_each(self.platforms, "clean")

    def start_workers(self):
        if self.workers:
            run_on_each(self.services, "stop_workers")
        self.workers = flatten_iterables(
            *run_on_each(self.services, "start_workers"))

    def submit(self, func, *args, **kwargs):
        worker = min(self.workers, key=Client.remaining_slot_num)
        return worker.async_call(func, *args, **kwargs)

    def async_map(self, func, *iterables):
        args_list = list(zip(*iterables))
        return [self.submit(func, *args) for args in args_list]

    def joinall(self, procs):
        for proc in procs:
            proc.join()

    def map(self, func, *iterables):
        procs = self.async_map(func, *iterables)
        self.joinall(procs)
        return [p.value for p in procs]
