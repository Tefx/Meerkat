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

    def clean(self):
        run_on_each(self.services, "clean")
        run_on_each(self.platforms, "clean")

    def start_workers(self, entry_points):
        if self.workers:
            run_on_each(self.services, "stop_workers")
        self.workers = flatten_iterables(
            *run_on_each(self.services, "start_workers", entry_points=entry_points))

    def map(self, func, *iterables):
        args_list = list(zip(*iterables))
        self.start_workers(func)
        results = []
        while args_list:
            current_results = []
            for worker, args in zip(self.workers, args_list):
                current_results.append(worker.async_call(func, *args))
            args_list = args_list[len(current_results):]
            for proc in current_results:
                proc.join()
                results.append(proc.value)
        return results
