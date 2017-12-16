import multiprocessing
import os
import os.path
import signal
import inspect

from .rpc import Port, RProc


DEFAULT_PORT = 8333


def get_module_name(obj):
    module_name = obj.__module__
    if module_name == "__main__":
        module_name = os.path.splitext(
            os.path.basename(inspect.getmodule(obj).__file__))[0]
    return module_name


def qualified_name(obj):
    if inspect.ismodule(obj):
        return obj.__name__
    else:
        module = inspect.getmodule(obj)
        for name in dir(module):
            if getattr(module, name) == obj:
                return "{}:{}".format(get_module_name(obj), name)


def func_full_name(func):
    if inspect.ismethod(func):
        return "{}.{}.{}".format(get_module_name(func),
                                 func.__self__.__class__.__name__,
                                 func.__name__)
    else:
        return "{}.{}".format(get_module_name(func),
                              func.__name__)


class Engine:
    def __init__(self, register=True):
        self.func_store = {}
        self.current_port = None
        if register:
            self.register(self._adm_hello)
            self.register(self._adm_list)
            self.register(self._adm_suspend)
            self.register(self._adm_resume)

    def register(self, func):
        func_name = func_full_name(func)
        # print(func_name, "registered")
        self.func_store[func_name] = func
        return func

    def wrapped_call(self, func_name, kwargs):
        func = self.func_store[func_name]
        for name, arg in kwargs.items():
            var_cls = func.__annotations__.get(name, None)
            if hasattr(var_cls, "__load__"):
                kwargs[name] = var_cls.__load__(arg)
        print("CALL", func, kwargs)
        res = func(**kwargs)
        if hasattr(res, "__dump__"):
            res = res.__dump__()
        return res

    def run(self, port=0, pipe=None):
        listener = Port.create_listener(port, pipe)
        while True:
            port = listener.accept()
            print("Receiving", port)
            proc = multiprocessing.Process(target=self.handle, args=(port, ))
            proc.start()

    def handle(self, port):
        while True:
            port.write(os.getpid())
            message = port.read()
            if message:
                self.current_port = port
                func_index, kwargs = message
                port.write(self.wrapped_call(func_index, kwargs))
            else:
                break

    def _adm_hello(self):
        return "Hello, {}:{}!".format(*self.current_port.peer_name)

    @staticmethod
    def _adm_suspend(pid):
        os.kill(pid, signal.SIGSTOP)

    @staticmethod
    def _adm_resume(pid):
        os.kill(pid, signal.SIGCONT)

    def _adm_list(self):
        return list(self.func_store.keys())

    def controller(self, ng_addr_str="localhost"):
        if ":" in ng_addr_str:
            ip, port = ng_addr_str.split(":")
            port = int(port)
        else:
            ip = ng_addr_str
            port = DEFAULT_PORT
        return Controller((ip, port))


class Controller:
    def __init__(self, ng_addr, keep_alive=False):
        self.keep_alive = keep_alive
        self.ng_addr = ng_addr
        self.running_set = []
        self.port = None
        if keep_alive:
            self.port = Port.create_connector(ng_addr, True)
        else:
            self.port = None

    def shutdown(self):
        if self.port:
            self.port.close()
            self.port = None

    def get_port(self, new_port=False):
        if not self.port or new_port:
            return Port.create_connector(self.ng_addr, False)
        return self.port

    def call(self, func, *args, **kwargs):
        port = self.get_port()
        func_name = func_full_name(func)
        return RProc(func, func_name, port)(*args, **kwargs)

    def async_call(self, func, *args, **kwargs):
        port = self.get_port()
        func_name = func_full_name(func)
        proc = RProc(func, func_name, port)
        proc.async_call(*args, **kwargs)
        return proc

    def __getattr__(self, adm_name):
        func = getattr(Engine(register=False), "_adm_{}".format(adm_name))
        port = self.get_port()
        func_name = func_full_name(func)
        return RProc(func, func_name, port)

    def __repr__(self):
        return "Controller[{}]".format(self.ng_addr)


def run_engine():
    import sys
    import argparse
    import importlib

    parser = argparse.ArgumentParser()
    parser.add_argument("entry_points", type=str, help="module:ng", nargs="?")
    parser.add_argument("-p", "--port", type=int,
                        default=DEFAULT_PORT, help="listening port")
    args = parser.parse_args()

    if not args.entry_points:
        ng = Engine()
    else:
        if ":" in args.entry_points:
            module_info, ng_name = args.entry_points.split(":")
        else:
            module_info, ng_name = args.entry_points, ""

        path, module_name = os.path.split(module_info)
        sys.path.insert(1, os.path.abspath(path))
        module = importlib.import_module(module_name)

        obj = getattr(module, ng_name, None)
        if isinstance(obj, Engine):
            ng = obj
        elif callable(obj):
            ng = Engine()
            ng.register(obj)
        elif obj == None:
            ng = Engine()
            for item in dir(module):
                if not item.startswith("__"):
                    item = getattr(module, item)
                    if callable(item):
                        ng.register(item)

    ng.run(port=args.port)


def gen_dockerfile():
    import argparse
    import os
    from subprocess import run
    from pkg_resources import resource_string

    parser = argparse.ArgumentParser()
    # parser.add_argument("entry_points", type=str, help="module:ng")
    parser.add_argument("-o", "--output", type=str, help="output file")
    parser.add_argument("-r", "--require", action="store_true",
                        help="generate requirements.txt")
    args = parser.parse_args()

    if os.path.exists("requirements.txt"):
        install_requirements = ""
    elif args.require:
        with open("requirements.txt", "w") as f:
            run(["pip", "freeze"], stdout=f)
        install_requirements = ""
    else:
        install_requirements = "#"

    temp = resource_string(__name__, "share/dockerfile/template").decode()
    content = temp.format(install_requirements=install_requirements)
    if args.output:
        with open(args.output, "w") as f:
            f.write(content)
    else:
        print(content)
