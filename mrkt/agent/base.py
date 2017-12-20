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


NULL_AGENT = Agent()
TASK_STATE_INIT= 0
TASK_STATE_READY = 1
TASK_STATE_RUNNING = 2
TASK_STATE_SUCCESS = 3
TASK_STATE_FAIL = 4


class Worker:
    def __init__(self, agent_addr, parallel_task_limit=None):
        self.agent_addr = agent_addr
        self.tasks = set()
        self.capacity = parallel_task_limit or self.cpu_count()
        self.ptask_semaphore = BoundedSemaphore(self.capacity)

    def utilization(self):
        return len(self.tasks) / self.capacity

    def wait_until_idle(self):
        if hasattr(self, "ptask_semaphore"):
            self.ptask_semaphore.acquire()

    def on_finish_task(self, task):
        self.tasks.remove(task)
        task.clean()
        if hasattr(self, "ptask_semaphore"):
            self.ptask_semaphore.release()

    def make_task(self, func):
        func_name = function_index(func)
        return Task(self.agent_addr, func, func_name, self)

    def exec(self, func, *args, **kwargs):
        task = self.make_task(func)
        return task(*args, **kwargs)

    def async_exec(self, func, *args, **kwargs):
        task = self.make_task(func)
        task.start(*args, **kwargs)
        return task

    def __getattr__(self, name):
        index = "_adm_{}".format(name)
        return Task(self.agent_addr, getattr(NULL_AGENT, index), index, self)

    def __repr__(self):
        return "Client[{}]".format(self.agent_addr)


class Task:
    def __init__(self, addr, func, func_name, worker):
        self.addr = addr
        self.func = func
        self.func_name = func_name
        self.worker = worker
        self.state = TASK_STATE_INIT
        self.port = None
        self.let = None
        self.tid = None
        self.ret = None

    def clean(self):
        if self.port:
            self.port.close()

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
        self.tid = self.port.read()
        while not self.tid and times:
            self.port.reconnect()
            self.tid = self.port.read()
            times -= 1
            gevent.sleep(intervals)

    def exec(self, *args, **kwargs):
        self.state = TASK_STATE_READY
        self.worker.wait_until_idle()
        self.state = TASK_STATE_RUNNING
        self.port = Port.create_connector(self.addr)
        self.wait_for_server()
        kwargs = self.dump_args(args, kwargs)
        if self.port.write((self.func_name, kwargs)):
            msg = self.port.read()
            if msg != None:
                self.ret = self.load_ret(msg)
                if isinstance(self.ret, CatchException):
                    self.state = TASK_STATE_FAIL
                    self.worker.on_finish_task(self)
                    self.ret.re_raise()
                else:
                    self.state = TASK_STATE_SUCCESS
                    self.worker.on_finish_task(self)
                    return
        self.state = TASK_STATE_FAIL

    def start(self, *args, **kwargs):
        self.worker.tasks.add(self)
        self.let = gevent.spawn(self.exec, *args, **kwargs)

    def __call__(self, *args, **kwargs):
        self.worker.tasks.add(self)
        self.exec(*args, **kwargs)
        return self.ret

    def join(self):
        self.let.join()
