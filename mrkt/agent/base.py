import inspect
import os
import os.path
import logging
import signal
import gevent
from multiprocessing import Process
from uuid import uuid1
import traceback
from gevent.lock import BoundedSemaphore

from .rpc import Port

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

    def _adm_suspend(self, uuid):
        p = self.processes.get(uuid, None)
        if p:
            os.kill(p.pid, signal.SIGSTOP)

    def _adm_resume(self, uuid):
        p = self.processes.get(uuid, None)
        if p:
            os.kill(p.pid, signal.SIGCONT)

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
            p = Process(target=self.invoke, args=(port, func, kwargs))
            setattr(p, "task", (index, func, kwargs))
            p.start()
            self.processes[uuid] = p
        else:
            logging.CRITICAL("[%s.Call]: cannot receive request!", self.__class__.__name__)


class Client:
    def __init__(self, agent_addr, parallel_task_limit=None):
        self.agent_addr = agent_addr
        self.running_tasks = set()
        self.ptask_lim = parallel_task_limit or self.cpu_count()
        self.ptask_semaphore = BoundedSemaphore(self.ptask_lim)

    def remaining_slot_num(self):
        return self.ptask_lim - len(self.running_tasks)

    def on_start_task(self, proc):
        if hasattr(self, "ptask_semaphore"):
            self.ptask_semaphore.acquire()
        self.running_tasks.add(proc)

    def on_finish_task(self, proc):
        self.running_tasks.remove(proc)
        if hasattr(self, "ptask_semaphore"):
            self.ptask_semaphore.release()

    def on_fail_task(self, proc):
        self.running_tasks.remove(proc)
        if hasattr(self, "ptask_semaphore"):
            self.ptask_semaphore.release()

    def call(self, func, *args, **kwargs):
        func_name = function_index(func)
        return RProc(self.agent_addr, func, func_name, self)(*args, **kwargs)

    def async_call(self, func, *args, **kwargs):
        func_name = function_index(func)
        proc = RProc(self.agent_addr, func, func_name, self)
        proc.async_call(*args, **kwargs)
        return proc

    def __getattr__(self, name):
        index = "_adm_{}".format(name)
        return RProc(self.agent_addr, getattr(Agent(), index), index, self)

    def __repr__(self):
        return "Client[{}]".format(self.agent_addr)


class RProc:
    def __init__(self, addr, func, func_name, worker):
        self.func = func
        self.func_name = func_name
        self.worker = worker
        self.port = Port.create_connector(addr)
        self.let = None
        self.rpid = None

    def dump_args(self, args, kwargs):
        args = inspect.signature(self.func).bind(*args, **kwargs)
        args.apply_defaults()
        kwargs = args.arguments
        for name, arg in kwargs.items():
            if hasattr(arg, "__dump__"):
                kwargs[name] = arg.__dump__()
        return kwargs

    def load_ret(self, ret):
        ret_cls = self.func.__annotations__.get("return")
        if ret_cls:
            ret = ret_cls.__load__(ret)
        return ret

    def wait_for_server(self, times=5, intervals=0.5):
        self.rpid = self.port.read()
        while not self.rpid and times:
            self.port.reconnect()
            self.rpid = self.port.read()
            times -= 1
            gevent.sleep(intervals)

    def __call__(self, *args, **kwargs):
        self.wait_for_server()
        self.worker.on_start_task(self)
        kwargs = self.dump_args(args, kwargs)
        if self.port.write((self.func_name, kwargs)):
            msg = self.port.read()
            if msg != None:
                ret = self.load_ret(msg)
                if isinstance(ret, CatchException):
                    self.worker.on_fail_task(self)
                    ret.re_raise()
                else:
                    self.worker.on_finish_task(self)
                    return ret
        self.worker.on_fail_task(self)

    def async_call(self, *args, **kwargs):
        self.let = gevent.spawn(self, *args, **kwargs)

    def join(self):
        self.let.join()

    @property
    def value(self):
        return self.let.value
