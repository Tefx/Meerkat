import struct
import inspect
from json import loads as load, dumps as dump
import gevent
import gevent.socket


HEADER_STRUCT = ">L"
HEADER_LEN = struct.calcsize(HEADER_STRUCT)


class Remotable:
    state = ()

    def __dump__(self):
        return [getattr(self, s) for s in self.state]

    @classmethod
    def __load__(cls, state):
        ins = cls.__new__(cls)
        for name, value in zip(cls.state, state):
            setattr(ins, name, value)
        return ins

    def __str__(self):
        return "{}<{}>".format(self.__class__.__name__, ",".join(
            "{}={}".format(s, getattr(self, s)) for s in self.state))


def safe_recv(sock, length):
    try:
        buf = sock.recv(length)
        if buf:
            return buf
    except OSError:
        sock.close()
        return False


def safe_send(sock, buf):
    try:
        sock.sendall(buf)
        return True
    except OSError:
        sock.close()
        return False


def try_connect(sock, addr, times, intervals):
    while times:
        try:
            sock.connect(addr)
            break
        except:
            gevent.sleep(intervals)
            times -= 1
    return times


class Port:
    def __init__(self, sock):
        self._sock = sock

    def __del__(self):
        self._sock.close()

    def read(self):
        header = safe_recv(self._sock, HEADER_LEN)
        if not header:
            return False
        length = struct.unpack(HEADER_STRUCT, header)[0]
        chunks = []
        while length:
            recv = safe_recv(self._sock, length)
            if not recv:
                return False
            chunks.append(recv)
            length -= len(recv)
        buf = b"".join(chunks).decode("utf-8")
        return load(buf)

    def write(self, buf):
        buf = dump(buf).encode("utf-8")
        msg = struct.pack(HEADER_STRUCT, len(buf)) + buf
        return safe_send(self._sock, msg)

    def close(self):
        self._sock.shutdown(gevent.socket.SHUT_RDWR)
        self._sock.close()

    @property
    def peer_name(self):
        return self._sock.getpeername()

    @classmethod
    def create_listener(cls, port=0, pipe=None):
        listen_sock = gevent.socket.socket(gevent.socket.AF_INET, gevent.socket.SOCK_STREAM)
        listen_sock.bind(("", port))
        listen_sock.listen(10000)
        if pipe:
            pipe.put(listen_sock.getsockname()[1])
        return cls(listen_sock)

    def accept(self):
        sock, _ = self._sock.accept()
        return self.__class__(sock)

    @classmethod
    def create_connector(cls, addr):
        sock = gevent.socket.socket(gevent.socket.AF_INET, gevent.socket.SOCK_STREAM)
        if try_connect(sock, addr, 10, 1):
            return cls(sock)

    def reconnect(self):
        addr = self._sock.getpeername()
        self._sock = gevent.socket.socket(gevent.socket.AF_INET, gevent.socket.SOCK_STREAM)
        self._sock.connect(addr)


class RProc:
    def __init__(self, addr, func, func_name):
        self.func = func
        self.func_name = func_name
        self.port = Port.create_connector(addr)
        self.let = None
        self.rpid = None

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
        self.rpid = self.port.read()
        while not self.rpid and times:
            self.port.reconnect()
            self.rpid = self.port.read()
            times -= 1
            gevent.sleep(intervals)

    def __call__(self, *args, **kwargs):
        self.wait_for_server()
        kwargs = self.dump_args(args, kwargs)
        if self.port.write((self.func_name, kwargs)):
            msg = self.port.read()
            if msg != None:
                return self.load_ret(msg)

    def async_call(self, *args, **kwargs):
        self.let = gevent.spawn(self, *args, ** kwargs)

    def join(self):
        self.let.join()

    @property
    def value(self):
        return self.let.value
