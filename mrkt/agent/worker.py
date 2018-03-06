import inspect
import os.path
from logging import getLogger
logger = getLogger(__name__)

import gevent
from gevent._semaphore import BoundedSemaphore

from .agent import DynamicAgent, CatchException, function_index
from mrkt.agent.rpc import Port
from .rdiff import dir_delta

NULL_AGENT = DynamicAgent()
TASK_STATE_UNASSIGNED = 0
TASK_STATE_READY = 1
TASK_STATE_RUNNING = 2
TASK_STATE_SUCCESS = 3
TASK_STATE_FAIL = 4


class Task:
    def __init__(self, func, args=None, kwargs=None, func_name=None):
        self.func = func
        self.func_name = func_name or function_index(func)
        self.args = (args or [], kwargs or {})
        self.state = TASK_STATE_UNASSIGNED
        self.addr = None
        self.port = None
        self.let = None
        self.tid = None
        self.ret = None

    def clean(self):
        if self.port:
            self.port.close()

    def assign_to(self, worker):
        self.addr = worker.agent_addr
        worker.tasks.add(self)
        self.let = gevent.spawn(self.execute, worker, *self.args)

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

    def execute(self, worker, args, kwargs):
        self.state = TASK_STATE_READY
        worker.wait_until_idle()
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
                    worker.on_finish_task(self)
                    self.ret.re_raise()
                else:
                    self.state = TASK_STATE_SUCCESS
                    worker.on_finish_task(self)
                return
            else:
                error_msg = "[{}]: No response from function invocation".format(self.func_name)
        else:
            error_msg = "[{}]: Cannot send function invocation request".format(self.func_name)
        self.state = TASK_STATE_FAIL
        worker.on_finish_task(self)
        raise OSError(error_msg)

    def join(self):
        while self.state == TASK_STATE_UNASSIGNED:
            gevent.sleep(0.1)
        self.let.join()

    def __repr__(self):
        return "[T/{}]<{}>".format(self.state, self.func_name)

    def is_adm_task(self):
        return self.func_name.startswith("_adm_")


class Worker:
    def __init__(self, agent_addr, parallel_task_limit=None):
        self.agent_addr = agent_addr
        self.tasks = set()
        self.ptask_semaphore = None
        self.capacity = parallel_task_limit or self.cpu_count()
        self.ptask_semaphore = BoundedSemaphore(self.capacity)
        self.sync_tag = 0
        self.sync_flag = False

    def utilization(self):
        return len([t for t in self.tasks if not t.is_adm_task()]) / self.capacity

    def is_available(self):
        return self.utilization() < 1

    def wait_until_idle(self):
        if self.ptask_semaphore is not None:
            self.ptask_semaphore.acquire()

    def on_finish_task(self, task):
        self.tasks.remove(task)
        task.clean()
        if self.ptask_semaphore is not None:
            self.ptask_semaphore.release()

    def __getattr__(self, name):
        index = "_adm_{}".format(name)
        func = getattr(NULL_AGENT, index)

        def adm_func(*args, **kwargs):
            task = Task(func, args, kwargs, index)
            task.assign_to(self)
            task.join()
            return task.ret

        return adm_func

    def __repr__(self):
        return "Client[{}]".format(self.agent_addr)

    def calculate_dir_delta(self, path):
        sig = self.dir_signature(path, os.path.isdir(path))
        logger.info("[Worker.Sync]%s: Got signture[size:%s]", self.agent_addr, len(sig))
        delta = dir_delta(sig, path)
        logger.info("[Worker.Sync]%s: Delta calculated[size:%s]", self.agent_addr, len(delta))
        return delta

    def sync_with_delta(self, delta, path):
        self.dir_patch(delta, path)
        logger.info("[Worker.Sync]%s: Patch finished", self.agent_addr)
        self.clean_cache()
        logger.info("[Worker.Sync]%s: Cache cleaned", self.agent_addr)
        self.sync_tag += 1

    def is_syncing(self):
        return self.sync_flag

    def set_syncing(self, value):
        self.sync_flag = value
