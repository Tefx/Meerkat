from gevent.monkey import patch_thread

patch_thread()
import argparse
import importlib
import inspect
import os
import os.path
import logging
import signal
import sys

import gevent
from multiprocessing import Process
from uuid import uuid1
import traceback

from .rpc import Port
from .rdiff import dir_sig, dir_patch

DEFAULT_PORT = 8333
PROCESS_CLEAN_INTERVAL = 5


class TaskFailure(Exception):
    pass


class CatchException:
    def __init__(self, exc):
        self.exc = exc
        self.tb = traceback.format_exc()

    def re_raise(self):
        print(self.tb)
        raise TaskFailure() from self.exc


def get_module_name(obj):
    module_name = obj.__module__
    if module_name == "__main__":
        module_name = os.path.splitext(
            os.path.basename(inspect.getmodule(obj).__file__))[0]
    return module_name


def function_index(func):
    if inspect.ismethod(func):
        func_name = "{}.{}".format(func.__self__.__class__.__name__, func.__name__)
    else:
        func_name = func.__name__
    return "{}:{}".format(get_module_name(func), func_name)


def index_split(index):
    if ":" in index:
        return index.split(":")
    else:
        return index, None


class Agent:
    def __init__(self):
        self.port = None
        self.function_store = {}
        self.register_adm_functions()
        self.processes = {}

    def register(self, func, index=None):
        index = index or function_index(func)
        logging.debug("[%s.LoadIntoCache]: %s", self.__class__.__name__, index)
        self.function_store[index] = func
        return func

    def register_adm_functions(self):
        for item in dir(self):
            if item.startswith("_adm"):
                self.register(getattr(self, item), item)

    def look_up_function(self, index):
        return self.function_store[index]

    def _adm_hello(self):
        return "Hello, {}:{}!".format(*self.port.peer_name)

    @staticmethod
    def _adm_cpu_count():
        return os.cpu_count()

    def _adm_suspend(self, uuid):
        p = self.processes.get(uuid, None)
        if p:
            os.kill(p.pid, signal.SIGSTOP)
        return True

    def _adm_resume(self, uuid):
        p = self.processes.get(uuid, None)
        if p:
            os.kill(p.pid, signal.SIGCONT)
        return True

    def _adm_list(self):
        return list(self.function_store.keys())

    def invoke(self, port, func, kwargs):
        self.port = port
        for name, arg in kwargs.items():
            var_cls = func.__annotations__.get(name, None)
            if hasattr(var_cls, "__load__"):
                kwargs[name] = var_cls.__load__(arg)
        try:
            res = func(**kwargs)
        except Exception as e:
            res = CatchException(e)
        logging.info("[%s.Result]: %s", self.__class__.__name__, res)
        if hasattr(res, "__dump__"):
            res = res.__dump__()
        port.write(res)

    def run(self, port=0, pipe=None):
        logging.info("[%s] stated on %s", self.__class__.__name__, port)
        listener = Port.create_listener(port, pipe)
        gevent.spawn(self.pool_cleaner)
        while True:
            port = listener.accept()
            gevent.spawn(self.request_handler, port)

    def pool_cleaner(self):
        while True:
            self.processes = {uuid: p for uuid, p in self.processes.items() if p.is_alive()}
            logging.info("[%s.Cleaner]: remaining %s tasks", self.__class__.__name__, len(self.processes))
            logging.debug("[%s.Cleaner]: %s", self.__class__.__name__, [(p, p.task) for p in self.processes.values()])
            gevent.sleep(PROCESS_CLEAN_INTERVAL)

    def request_handler(self, port):
        logging.info("[%s.Request]: %s", self.__class__.__name__, port)
        uuid = uuid1().int
        port.write(uuid)
        message = port.read()
        if message:
            index, kwargs = message
            func = self.look_up_function(index)
            logging.info("[%s.Call]: %s on %s",
                         self.__class__.__name__, index, kwargs)
            if not index.startswith("_adm_"):
                p = Process(target=self.invoke, args=(port, func, kwargs))
                setattr(p, "task", (index, func, kwargs))
                p.start()
                self.processes[uuid] = p
            else:
                self.invoke(port, func, kwargs)
        else:
            logging.CRITICAL("[%s.Call]: cannot receive request!", self.__class__.__name__)


class DynamicAgent(Agent):
    def __init__(self, path=None):
        super(DynamicAgent, self).__init__()
        self.module_cache = {}
        if path:
            self.path = os.path.abspath(path)
            self.setup_path(path)

    def setup_path(self, path):
        sys.path.insert(1, os.path.abspath(path))

    def look_up_function(self, index):
        if index not in self.function_store:
            module_name, func_name = index_split(index)
            if module_name not in self.module_cache:
                self.module_cache[module_name] = importlib.import_module(
                    module_name)
            func = getattr(self.module_cache[module_name], func_name)
            self.register(func, index)
        return self.function_store[index]

    def _adm_dir_signature(self):
        return dir_sig(self.path)

    def _adm_dir_patch(self, delta):
        return dir_patch(self.path, delta)

    def _adm_clean_cache(self):
        for module_name, module in list(self.module_cache.items()):
            self.module_cache[module_name] = importlib.reload(module)
        self.function_store = {}
        self.register_adm_functions()
        return True

    @classmethod
    def launch(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument("path", type=str, help="path",
                            nargs="?", default=".")
        parser.add_argument("-p", "--port", type=int,
                            help="port", default=DEFAULT_PORT)
        parser.add_argument("-l", "--logging", type=str,
                            help="Logging level", default="warning")
        args = parser.parse_args()
        logging.basicConfig(level=getattr(logging, args.logging.upper()))
        cls(args.path).run(port=args.port)
