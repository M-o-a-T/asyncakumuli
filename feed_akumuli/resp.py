#
# Stream adapter for RESP

from .buffered import BufferedReader
from .model import Entry, EntryDelta, tags2str, str2tags
import trio
from trio.abc import AsyncResource
import datetime
from contextlib import asynccontextmanager
import json
import math
import heapq
import time

EOL=b'\r\n'

_url="http://stats.w.smurf.noris.de:8181/api/query"

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

async def get_min_ts(q, series, tags):
    """
    Read the timestamp of the last entry.
    """
    r = await q.post(_url, data=json.dumps(dict(aggregate={series:"last"},
        where=tags)))
    if r.raw:
        br = Resp(BufferedReader(data=r.raw))
        try:
            tag = await br.receive()
        except RespError as exc:
            if exc.args[0] == "not found" or exc.args[0] == "":
                return -math.inf
            raise
        else:
            if r.status_code != 200:
                raise RespError(r.reason_phrase, r.raw)
        time = parse_timestamp(await br.receive())
        val = await br.receive()
        return time.timestamp()
    else:
        return -math.inf

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
    """
    This class implements Akumuli's RESP submission protocol.

    If "delta" is True, uses an EntryDelta filter to skip runs of
    consecutive values if appropriate.
    """
    def __init__(self, stream, delay=15, delta=False):
        if not isinstance(stream, BufferedReader):
            stream = BufferedReader(stream)
        self.stream = stream
        self.buf = []
        self._dict = {}
        self._dt = 0
        self._heap = []
        self._same = EntryDelta() if delta else lambda x:x
        self._delay = delay
        self._large: trio.Event = None
        self._sender: trio.Event = None
        self._done: trio.Event = None

    def preload(self, series, tags):
        self._dt += 1
        if not isinstance(tags,str):
            raise RuntimeError("want a string")
        self._dict.setdefault(series,{})[tags] = self._dt

    async def flush_dict(self):
        di = []
        for k,v in self._dict.items():
            for kk,vv in v.items():
                di.append("%s %s" % (k,kk))
                di.append(vv)
        await self.send(di)

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
        """Send these raw data"""
        self._resp_encode(data, join=join)
        if len(self.buf) > 1000:
            await self.flush()

    async def write(self, pkt:Entry):
        """Send the contents of this packet"""
        tags = pkt.tags_str
        try:
            dt = self._dict[pkt.series][tags]
        except KeyError:
            dt = "%s %s" % (pkt.series, tags)
        await self.send([dt, int(pkt.time*1000000000), pkt.value], join=True)

    async def put(self, pkt):
        """Store this packet, for eventual sending.
        WARNING: there is no flow control here.
        """
        heapq.heappush(self._heap, pkt)
        if self._sender is not None:
            self._sender.set()

        if self._large is None and len(self._heap) > 100:
            self._large = trio.Event()
        if self._large is not None:
            await self._large.wait()

    async def next_to_send(self):
        while True:
            while self._heap:
                if self._large is not None and len(self._heap) < 50:
                    self._large.set()
                    self._large = None

                self._sender = None
                if self._heap[0].time <= self._t-self._delay:
                    return heapq.heappop(self._heap)
                if self._done is not None:
                    self._done.set()
                    return
                self._t = time.time()
                if self._heap[0].time <= self._t-self._delay:
                    return heapq.heappop(self._heap)
                await trio.sleep(self._delay+self._heap[0].time-self._t)

            if self._done is not None:
                self._done.set()
                return
            if self._sender is None:
                self._sender = trio.Event()
            await self._sender.wait()

    async def send_all(self, task_status):
        self._t = time.time()
        task_status.started()

        while True:
            e = await self.next_to_send()
            if e is None:
                return
            e = self._same(e)
            if e is not None:
                await self.write(e)

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
async def connect(nursery, host="127.0.0.1", port=8282, **kw):
    """A context manager for managing the Resp object"""
    async with await trio.open_tcp_stream(host, port) as s:
        s = Resp(s, **kw)
        ss = await nursery.start(s.send_all)
        cs = await nursery.start(reader, s)
        yield s

        s._done = trio.Event()
        if s._sender is None:
            s._sender = trio.Event()
        s._sender.set()
        await s._done.wait()

        await trio.sleep(0.3) # let's hope that's enough to read errors
        cs.cancel()

