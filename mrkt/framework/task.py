import inspect
import gevent
from enum import Enum

from ..common.exceptions import ExceptionCaught
from ..common.port import Port
from ..common.utils import function_index
from ..common.consts import *


class Task:
    State = Enum("State", "Waiting Ready Running Succeed Failed")

    def __init__(self, func, args=None, kwargs=None, func_name=None):
        self.func = func
        self.func_name = func_name or function_index(func)
        self.args = (args or [], kwargs or {})
        self.state = Task.State.Waiting
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

    def wait_for_server(self, times=TASK_WAIT_PID_RETRY_TIMES, intervals=TASK_WAIT_PID_RETRY_INTERVAL):
        self.tid = self.port.read()
        while not self.tid and times:
            self.port.reconnect()
            self.tid = self.port.read()
            times -= 1
            gevent.sleep(intervals)

    def execute(self, worker, args, kwargs):
        self.state = Task.State.Ready
        worker.wait_until_idle()
        self.state = Task.State.Running
        self.port = Port.create_connector(self.addr)
        self.wait_for_server()
        kwargs = self.dump_args(args, kwargs)
        if self.port.write((self.func_name, kwargs)):
            msg = self.port.read()
            if msg != None:
                self.ret = self.load_ret(msg)
                if isinstance(self.ret, ExceptionCaught):
                    self.state = Task.State.Failed
                    worker.on_finish_task(self)
                    self.ret.re_raise()
                else:
                    self.state = Task.State.Succeed
                    worker.on_finish_task(self)
                return
            else:
                error_msg = "[{}]: No response from function invocation".format(self.func_name)
        else:
            error_msg = "[{}]: Cannot send function invocation request".format(self.func_name)
        self.state = Task.State.Failed
        worker.on_finish_task(self)
        raise OSError(error_msg)

    def join(self):
        while self.state == Task.State.Waiting:
            gevent.sleep(CLUSTER_SCHEDULE_INTERVAL)
        self.let.join()

    def __repr__(self):
        return "[T/{}]<{}>".format(self.state, self.func_name)

    def is_adm_task(self):
        return self.func_name.startswith("_adm_")
