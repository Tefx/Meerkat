from gevent.monkey import patch_thread;

patch_thread()
import gevent
import argparse
import importlib
import os
import os.path
import logging
import signal
import sys
from multiprocessing import Process
from uuid import uuid1

from ..common.consts import AGENT_PORT, AGENT_CLEAN_PROCESS_INTERVAL
from ..common.exceptions import ExceptionCaught
from ..common.port import Port
from ..common.rdiff import dir_sig, dir_patch
from ..common.utils import function_index, index_split


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
            res = ExceptionCaught(e)
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
            gevent.sleep(AGENT_CLEAN_PROCESS_INTERVAL)

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
                p = Process(target=self.invoke, name="mrtk_t{}".format(uuid), args=(port, func, kwargs))
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

    def _adm_dir_signature(self, subpath, is_dir=True):
        return dir_sig(os.path.join(self.path, subpath), is_dir)

    def _adm_dir_patch(self, delta, subpath):
        return dir_patch(os.path.join(self.path, subpath), delta)

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
                            help="port", default=AGENT_PORT)
        parser.add_argument("-l", "--logging", type=str,
                            help="Logging level", default="warning")
        args = parser.parse_args()
        logging.basicConfig(level=getattr(logging, args.logging.upper()))
        cls(args.path).run(port=args.port)
