import struct
import gevent.socket
import gevent
from dill import loads, dumps

from .consts import PORT_CONNECT_RETRIES, PORT_CONNECT_RETRY_INTERVAL

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
        if not buf:
            raise OSError("port failed to receive data")
        return buf
    except OSError as e:
        sock.close()
        raise OSError("port failed to receive data") from e


def safe_send(sock, buf):
    try:
        sock.sendall(buf)
        return True
    except OSError as e:
        sock.close()
        raise OSError("port failed to send data") from e


def try_connect(sock, addr, times, intervals):
    while times:
        try:
            sock.connect(addr)
            break
        except OSError:
            gevent.sleep(intervals)
            times -= 1
    return times


class ObjPort:
    def __init__(self, sock):
        self._sock = sock
        self.address = None

    def __del__(self):
        self._sock.close()

    def read(self):
        header = safe_recv(self._sock, HEADER_LEN)
        length = struct.unpack(HEADER_STRUCT, header)[0]
        chunks = []
        while length:
            recv = safe_recv(self._sock, length)
            chunks.append(recv)
            length -= len(recv)
        buf = b"".join(chunks)
        return loads(buf)

    def write(self, buf):
        buf = dumps(buf)
        if not isinstance(buf, bytes):
            buf = buf.encode("utf-8")
        msg = struct.pack(HEADER_STRUCT, len(buf)) + buf
        return safe_send(self._sock, msg)

    def close(self):
        try:
            self._sock.shutdown(gevent.socket.SHUT_RDWR)
            self._sock.close()
        except OSError:
            pass

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
        if try_connect(sock, addr, PORT_CONNECT_RETRIES, PORT_CONNECT_RETRY_INTERVAL):
            port = cls(sock)
            port.address = sock.getpeername()
        else:
            raise OSError("Create port failed.")
        return port

    def reconnect(self):
        self._sock = gevent.socket.socket(gevent.socket.AF_INET, gevent.socket.SOCK_STREAM)
        self._sock.connect(self.address)
