from itertools import chain
import gevent


def flatten_iterables(*iterables):
    return list(chain(*iterables))


def parallel_run(iterable, method, *args, **kwargs):
    lets = [gevent.spawn(getattr(item, method), *args, **kwargs)
            for item in iterable]
    gevent.joinall(lets)
    return [let.value for let in lets]
