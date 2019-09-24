#
# Stream adapter for RESP

from .buffered import BufferedReader
import trio
from trio.abc import AsyncResource
import datetime
from contextlib import asynccontextmanager

EOL=b'\r\n'

class NoCodeError(RuntimeError):
    """Could not encode this"""
    def __init__(self, data):
        self.data = data
    def __repr__(self):
        return "<%s:%r>" % (self.__class__.__name__, self.data)
    def __str__(self):
        return "Cannot encode %s" % (self.data.__class__.__name__,)
    
class RespError(RuntimeError):
    """Received an error"""
    def __str__(self):
        return "Akumuli: error: "+" ".join(str(x) for x in self.args)

class RespUnknownError(RespError):
    """Received an unknown line"""
    pass

def parse_timestamp(ts):
    """Parse ISO formatted timestamp"""
    try:
        return datetime.datetime.strptime(ts.rstrip('0').rstrip('.')+" +0000", "%Y%m%dT%H%M%S.%f %z")
    except ValueError:
        return datetime.datetime.strptime(ts.rstrip('0').rstrip('.')+" +0000", "%Y%m%dT%H%M%S %z")

def _resp_encode(buf, data):
    if isinstance(data, str) and '\r' not in data and '\n' not in data:
        buf.append(b'+'+str(data).encode("utf-8")+EOL)
    elif isinstance(data, int):
        buf.append(b':'+str(data).encode("ascii")+EOL)
    elif isinstance(data, float):
        buf.append(b'+'+str(data).encode("ascii")+EOL)
    elif isinstance(data, BaseException):
        buf.append(b'-'+str(data).encode("utf-8")+EOL)
    elif isinstance(data, (list,tuple)):
        buf.append(b'*'+str(len(data)).encode("ascii")+EOL)
        for d in data:
            _resp_encode(buf, d)
    elif isinstance(data, bytes):
        buf.append('$'+str(len(data)).encode("ascii")+EOL) 
        buf.append(data)
        buf.append(EOL)
    else:
        raise NoCodeError(data)

class Resp(AsyncResource):
    def __init__(self, stream):
        if not isinstance(stream, BufferedReader):
            stream = BufferedReader(stream)
        self.stream = stream
        self.buf = []

    async def aclose(self):
        await self.flush()
        await self.stream.aclose()

    def _resp_encode(self, data, join=False):
        if join:
            assert isinstance(data,(tuple,list))
            for d in data:
                _resp_encode(self.buf, d)
        else:
            _resp_encode(self.buf, data)

    async def send(self, data, join=False):
        self._resp_encode(data, join=join)
        if len(self.buf) > 1000:
            await self.flush()

    async def flush(self):
        buf = b''.join(self.buf)
        self.buf = []
        await self.stream.send_all(buf)

    async def receive(self):
        line = await self.stream.readuntil(EOL)
        if not line:
            return None
        if isinstance(line,str):
            if line[0] == '+':
                return line[1:-len(EOL)]
            elif line[0] == ':':
                return int(line[1:-len(EOL)])
            elif line[0] == '-':
                raise RespError(line[1:-len(EOL)], self.stream.read_buffer)
            elif line[0] == '*':
                n = int(line[1:-len(EOL)])
                res = [ self.read() for _ in range(n) ]
                return res
            # '$' not supported for char buffers
            else:
                raise RespUnknownError(line[:-len(EOL)])
        else:
            if line[0] == b'+'[0]:
                return line[1:-len(EOL)].decode("utf-8")
            elif line[0] == b':'[0]:
                return int(line[1:-len(EOL)])
            elif line[0] == b'-'[0]:
                raise RespError(line[1:-len(EOL)].decode("utf-8"), self.stream.read_buffer)
            elif line[0] == b'*'[0]:
                n = int(line[1:-len(EOL)])
                res = [ self.read() for _ in range(n) ]
                return res
            elif line[0] == b'$'[0]:
                lb = int(line[1:-len(EOL)])
                b = self.stream.readexactly(lb)
                eb = self.stream.readexactly(len(EOL))
                assert eb == EOL, eb
                return b
            else:
                raise RespUnknownError(line[:-len(EOL)])

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            r = await self.receive()
        except trio.ClosedResourceError:
            raise StopAsyncIteration from None
        if r is None:
            raise StopAsyncIteration
        return r

async def reader(s, task_status=None):             
    with trio.CancelScope() as cs:
        task_status.started(cs)
        async for m in s:
            raise RuntimeError(m)         

@asynccontextmanager
async def connect(nursery, host="127.0.0.1", port=8282):
    async with await trio.open_tcp_stream(host, port) as s:
        s = Resp(s)
        cs = await nursery.start(reader, s)
        yield s
        await trio.sleep(0.3) # let's hope that's enough
        cs.cancel()

