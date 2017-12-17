import inspect
import os
import os.path
import logging
import signal
import gevent

from .rpc import Port, RProc

DEFAULT_PORT = 8333


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
        self.current_port = None
        self.function_store = {}
        self.register_adm_functions()

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
        return "Hello, {}:{}!".format(*self.current_port.peer_name)

    @staticmethod
    def _adm_suspend(pid):
        os.kill(pid, signal.SIGSTOP)

    @staticmethod
    def _adm_resume(pid):
        os.kill(pid, signal.SIGCONT)

    def _adm_list(self):
        return list(self.function_store.keys())

    def invoke(self, func, kwargs):
        for name, arg in kwargs.items():
            var_cls = func.__annotations__.get(name, None)
            if hasattr(var_cls, "__load__"):
                kwargs[name] = var_cls.__load__(arg)
        res = func(**kwargs)
        if hasattr(res, "__dump__"):
            res = res.__dump__()
        return res

    def run(self, port=0, pipe=None):
        logging.info("[%s] stated on %s", self.__class__.__name__, port)
        listener = Port.create_listener(port, pipe)
        while True:
            port = listener.accept()
            logging.info("[Request]: %s", port)
            gevent.spawn(self.request_handler, port)

    def request_handler(self, port):
        while True:
            self.current_port = port
            port.write(os.getpid())
            message = port.read()
            if message:
                index, kwargs = message
                func = self.look_up_function(index)
                logging.info("[%s.Call]: %s on %s",
                             self.__class__.__name__, index, kwargs)
                port.write(self.invoke(func, kwargs))
            else:
                break


class Client:
    def __init__(self, agent_addr, keep_alive=False):
        self.keep_alive = keep_alive
        self.agent_addr = agent_addr
        self.running_set = []
        self.port = None
        if keep_alive:
            self.port = Port.create_connector(agent_addr, True)
        else:
            self.port = None

    def shutdown(self):
        if self.port:
            self.port.close()
            self.port = None

    def get_port(self, new_port=False):
        if not self.port or new_port:
            return Port.create_connector(self.agent_addr, False)
        return self.port

    def call(self, func, *args, **kwargs):
        port = self.get_port()
        func_name = function_index(func)
        return RProc(func, func_name, port)(*args, **kwargs)

    def async_call(self, func, *args, **kwargs):
        port = self.get_port()
        func_name = function_index(func)
        proc = RProc(func, func_name, port)
        proc.async_call(*args, **kwargs)
        return proc

    def __getattr__(self, name):
        index = "_adm_{}".format(name)
        return RProc(getattr(Agent(), index), index, self.get_port())

    def __repr__(self):
        return "Client[{}]".format(self.agent_addr)
