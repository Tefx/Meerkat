import inspect
# import os
import os.path
import logging
# import signal
import gevent
from multiprocessing import Process
from uuid import uuid1

from .rpc import Port, RProc

DEFAULT_PORT = 8333
PROCESS_CLEAN_INTERVAL = 5


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
        logging.info("[%s.LoadIntoCache]: %s", self.__class__.__name__, index)
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

    # def _adm_suspend(self, uuid):
    #     p = self.processes.get(uuid, None)
    #     if p:
    #         os.kill(p.pid, signal.SIGSTOP)
    #
    # def _adm_resume(self, uuid):
    #     p = self.processes.get(uuid, None)
    #     if p:
    #         os.kill(p.pid, signal.SIGCONT)

    def _adm_list(self):
        return list(self.function_store.keys())

    def invoke(self, port, func, kwargs):
        self.port = port
        for name, arg in kwargs.items():
            var_cls = func.__annotations__.get(name, None)
            if hasattr(var_cls, "__load__"):
                kwargs[name] = var_cls.__load__(arg)
        res = func(**kwargs)
        logging.info("[%s.Invoke]: %s", self.__class__.__name__, res)
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
            gevent.sleep(PROCESS_CLEAN_INTERVAL)

    def request_handler(self, port):
        logging.info("[Request]: %s", port)
        uuid = uuid1().int
        port.write(uuid)
        message = port.read()
        if message:
            index, kwargs = message
            func = self.look_up_function(index)
            logging.info("[%s.Call]: %s on %s",
                         self.__class__.__name__, index, kwargs)
            p = Process(target=self.invoke, args=(port, func, kwargs))
            p.start()
            self.processes[uuid] = p
            # self.invoke(port, func, kwargs)
        else:
            logging.CRITICAL("[%s.Call]: cannot receive request!", self.__class__.__name__)


class Client:
    def __init__(self, agent_addr):
        self.agent_addr = agent_addr

    def call(self, func, *args, **kwargs):
        func_name = function_index(func)
        return RProc(self.agent_addr, func, func_name)(*args, **kwargs)

    def async_call(self, func, *args, **kwargs):
        func_name = function_index(func)
        proc = RProc(self.agent_addr, func, func_name)
        proc.async_call(*args, **kwargs)
        return proc

    def __getattr__(self, name):
        index = "_adm_{}".format(name)
        return RProc(self.agent_addr, getattr(Agent(), index), index)

    def __repr__(self):
        return "Client[{}]".format(self.agent_addr)
