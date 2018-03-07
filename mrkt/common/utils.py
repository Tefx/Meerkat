import inspect
import os

import gevent


def call_on_each(iterable, method, callback=None, join=False, **kwargs):
    def _let(obj):
        res = getattr(obj, method)(**kwargs)
        if callback:
            callback(res)

    lets = [gevent.spawn(_let, obj) for obj in iterable]
    if join:
        gevent.joinall(lets)


def index_split(index):
    if ":" in index:
        return index.split(":")
    else:
        return index, None


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
