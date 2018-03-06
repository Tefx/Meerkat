import gevent


def call_on_each(iterable, method, callback=None, join=False, **kwargs):
    def _let(obj):
        res = getattr(obj, method)(**kwargs)
        if callback:
            callback(res)

    lets = [gevent.spawn(_let, obj) for obj in iterable]
    if join:
        gevent.joinall(lets)
