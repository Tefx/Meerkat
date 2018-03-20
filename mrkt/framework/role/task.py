import inspect
import gevent
from enum import Enum

from mrkt.common.exceptions import TaskFailure
from mrkt.common.port import ObjPort
from mrkt.common.utils import function_index
from mrkt.common.consts import *


class Task:
    State = Enum("State", "Waiting Ready Running Succeed Failed")

    def __init__(self, func, args=None, kwargs=None, func_name=None):
        self.func = func
        self.func_name = func_name or function_index(func)
        self.args = (args or [], kwargs or {})
        self.state = Task.State.Waiting
        self.worker_address = None
        self.port = None
        self.let = None
        self.tid = None
        self.ret = None
        self.debug_stat = 0

    def clean(self):
        if self.port:
            self.port.close()

    def assign_to(self, worker):
        self.worker_address = worker.agent_addr
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

    def wait_for_server(self, times=TASK_WAIT_PID_RETRIES, interval=TASK_WAIT_PID_RETRY_INTERVAL):
        try:
            self.tid = self.port.read()
        except OSError as e:
            if times:
                gevent.sleep(interval)
                self.port.reconnect()
                self.wait_for_server(times - 1, interval)
            else:
                raise e

    def execute(self, worker, args, kwargs):
        self.state = Task.State.Ready
        worker.wait_until_idle()
        self.state = Task.State.Running
        self.port = ObjPort.create_connector(self.worker_address)
        self.wait_for_server()
        kwargs = self.dump_args(args, kwargs)
        try:
            self.debug_stat = 1
            self.port.write((self.func_name, kwargs))
            self.debug_stat = 2
            msg = self.port.read()
            self.ret = self.load_ret(msg)
            self.debug_stat = 3
            if isinstance(self.ret, TaskFailure):
                self.state = Task.State.Failed
                worker.on_finish_task(self)
                self.ret.re_raise()
            else:
                self.state = Task.State.Succeed
                worker.on_finish_task(self)
        except OSError as e:
            self.state = Task.State.Failed
            worker.on_finish_task(self)
            raise e

    def join(self):
        while self.state == Task.State.Waiting:
            gevent.sleep(CLUSTER_SCHEDULE_INTERVAL)
        self.let.join()

    def kill(self):
        if self.let:
            self.let.kill()
        self.clean()

    def __repr__(self):
        return "[T/{}]<{}>".format(self.state, self.func_name)

    def is_adm_task(self):
        return self.func_name.startswith("_adm_")
