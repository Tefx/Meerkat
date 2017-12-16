from math import ceil
from . import service
from . import platform
from .utils import flatten_iterables, parallel_run


class Cluster:
    def __init__(self, servers):
        self.servers = [s for s in servers if isinstance(s, service.BaseService)]
        self.platforms = [s for s in servers if isinstance(
            s, platform.BasePlatform)]
        servers_on_platforms = parallel_run(self.platforms, "servers")
        self.servers.extend(flatten_iterables(*servers_on_platforms))
        parallel_run(self.servers, "connect")

    def start_workers(self, entry_points, num):
        num_pre_server = ceil(num / len(self.servers))
        workers = parallel_run(
            self.servers, "start_workers", entry_points, num_pre_server)
        return flatten_iterables(*workers)

    def install_image(self, *args, **kwargs):
        parallel_run(self.servers, "install_image", *args, **kwargs)

    def uninstall_image(self):
        parallel_run(self.servers, "uninstall_image")

    def clean(self, uninstall_image=True):
        parallel_run(self.servers, "clean", uninstall_image=uninstall_image)
        parallel_run(self.platforms, "clean")

    def map(self, func, *iterables):
        args_list = list(zip(*iterables))
        workers = self.start_workers(func, len(args_list))
        results = []
        while args_list:
            current_results = []
            for worker, args in zip(workers, args_list):
                current_results.append(worker.async_call(func, *args))
            args_list = args_list[len(current_results):]
            for proc in current_results:
                proc.join()
                results.append(proc.value)
        return results
