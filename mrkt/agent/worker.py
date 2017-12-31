import inspect
import logging
import os.path

import gevent
from gevent._semaphore import BoundedSemaphore

from .agent import DynamicAgent, CatchException, function_index
from .rpc import Port
from .rdiff import dir_delta

NULL_AGENT = DynamicAgent()
TASK_STATE_INIT = 0
TASK_STATE_READY = 1
TASK_STATE_RUNNING = 2
TASK_STATE_SUCCESS = 3
TASK_STATE_FAIL = 4


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
            else:
                error_msg = "[{}]: No response from function invocation".format(self.func_name)
        else:
            error_msg = "[{}]: Cannot send function invocation request".format(self.func_name)
        self.state = TASK_STATE_FAIL
        self.worker.on_finish_task(self)
        raise OSError(error_msg)

    def start(self, *args, **kwargs):
        self.worker.tasks.add(self)
        self.let = gevent.spawn(self.exec, *args, **kwargs)

    def __call__(self, *args, **kwargs):
        self.worker.tasks.add(self)
        self.exec(*args, **kwargs)
        return self.ret

    def join(self):
        self.let.join()

    def __repr__(self):
        return "[T/{}]<{}>".format(self.state, self.func_name)

    def is_adm_task(self):
        return self.func_name.startswith("_adm_")


class Worker:
    def __init__(self, agent_addr, parallel_task_limit=None):
        self.agent_addr = agent_addr
        self.tasks = set()
        self.capacity = parallel_task_limit or self.cpu_count()
        self.ptask_semaphore = BoundedSemaphore(self.capacity)

    def utilization(self):
        return len([t for t in self.tasks if not t.is_adm_task()])/ self.capacity

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

    def sync_dir_delta(self, local_path, remote_path):
        sig = self.dir_signature(remote_path, os.path.isdir(local_path))
        logging.info("[Worker.Sync]: Got signture[size:%s]", len(sig))
        delta = dir_delta(sig, local_path)
        logging.info("[Worker.Sync]: Delta calculated[size:%s]", len(delta))
        return delta

    def sync_dir_patch(self, delta, remote_path):
        self.dir_patch(delta, remote_path)
        logging.info("[Worker.Sync]%s: Patch finished", self.agent_addr)
        self.clean_cache()
        logging.info("[Worker.Sync]%s: Cache cleaned", self.agent_addr)
