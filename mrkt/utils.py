from itertools import chain
import gevent


def flatten_iterables(*iterables):
    return list(chain(*iterables))


def set_option(obj, field, default, options):
    if field in options:
        setattr(obj, field, options.get(field))
    elif not hasattr(obj, field):
        setattr(obj, field, default)


def run_on_each(iterable, method, async=True, **kwargs):
    if not async:
        results = []
        for item in iterable:
            results.append(getattr(item, method)(**kwargs))
        return results
    else:
        lets = [gevent.spawn(getattr(item, method), **kwargs)
                for item in iterable]
        gevent.joinall(lets)
        return [let.value for let in lets]
