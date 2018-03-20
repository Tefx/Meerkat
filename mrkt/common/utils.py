import inspect
import os
import subprocess
import gevent
from gevent.pool import Group
from functools import wraps
from lz4.frame import compress, decompress

from .consts import RDIFF_SIG_FILENAME, RDIFF_DELTA_FILENAME


def patch():
    from threading import current_thread
    from gevent.monkey import patch_all
    patch_all(thread=current_thread().name == "MainThread")


def call_on_each(iterable, method, join=False, **kwargs):
    group = Group()
    for obj in iterable:
        group.spawn(getattr(obj, method), **kwargs)
    if join:
        group.join()


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


def dir_sig(path, is_dir=True):
    if not os.path.exists(path):
        if is_dir:
            os.makedirs(path)
        else:
            parent_dir = os.path.dirname(path)
            if not os.path.exists(parent_dir):
                os.makedirs(parent_dir)
            os.mknod(path)
    p = subprocess.run(["rdiffdir", "sig", path, "-"],
                       stdout=subprocess.PIPE)
    return compress(p.stdout)


def dir_delta(sig, new_path):
    with open(RDIFF_SIG_FILENAME, "wb") as f:
        f.write(decompress(sig))
    p = subprocess.run(["rdiffdir", "delta", RDIFF_SIG_FILENAME, new_path, "-"],
                       stdout=subprocess.PIPE,
                       input=sig)
    os.remove(RDIFF_SIG_FILENAME)
    return compress(p.stdout)


def dir_patch(path, delta):
    with open(RDIFF_DELTA_FILENAME, "wb") as f:
        f.write(decompress(delta))
    subprocess.run(["rdiffdir", "patch", path, RDIFF_DELTA_FILENAME])
    os.remove(RDIFF_DELTA_FILENAME)
    return True


def listify(typ=list):
    def _wrapper(func):
        @wraps(func)
        def _wrapped(*args, **kwargs):
            return typ(func(*args, **kwargs))

        return _wrapped

    return _wrapper
