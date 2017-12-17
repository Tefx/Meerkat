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

    def map(self, func, *iterables):
        args_list = list(zip(*iterables))
        results = []
        while args_list:
            procs = []
            for worker, args in zip(self.workers, args_list):
                procs.append(worker.async_call(func, *args))
            args_list = args_list[len(procs):]
            for proc in procs:
                proc.join()
                results.append(proc.value)
        return results
