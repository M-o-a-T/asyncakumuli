#
# Stream adapter for RESP

from .buffered import BufferedReader, IncompleteReadError
from .model import Entry, EntryDelta, tags2str
import trio
from trio.abc import AsyncResource
from contextlib import asynccontextmanager
import json
import math
import heapq
import time
from typing import Union, Iterable, List, Mapping
from datetime import datetime

EOL = b"\r\n"

_url = "http://localhost:8181/api/query"


class NoCodeError(RuntimeError):
    """Could not encode this"""

    def __init__(self, data):
        super().__init__()
        self.data = data

    def __repr__(self):
        return "<%s:%r>" % (self.__class__.__name__, self.data)

    def __str__(self):
        return "Cannot encode %s" % (self.data.__class__.__name__,)


class RespError(RuntimeError):
    """Received an error"""

    def __str__(self):
        return "Akumuli: error: " + " ".join(
            str(x) for x in self.args  # pylint: disable=not-an-iterable
        )


class RespUnknownError(RespError):
    """Received an unknown line"""

    pass


RespType = Union[str, int, float, Iterable["RespType"]]
RespTags = Mapping[str, str]
ExtRespType = Union[BaseException, str, int, float, Iterable[RespType]]


async def get_max_ts(asks_session, series: str, tags: dict = (), url: str = _url):
    """
    Read the timestamp of the last entry.
    """
    if not tags:
        tags = {}

    r = await asks_session.post(url, data=json.dumps(dict(aggregate={series: "last"}, where=tags)))
    if r.raw:
        br = Resp(BufferedReader(data=r.raw))
        try:
            await br.receive()  # ignore value
        except RespError as exc:
            if exc.args[0] == "not found" or exc.args[0] == "":
                return -math.inf
            raise
        else:
            if r.status_code != 200:
                raise RespError(r.reason_phrase, r.raw)
        tm = parse_timestamp(await br.receive())
        await br.receive()  # ignore value
        return tm.timestamp()
    else:
        return -math.inf


async def get_data(
    asks_session, series: str, tags: dict, t_start=None, t_end=None, url: str = _url
):
    """
    Read some data.
    """
    r = {}
    if t_start:
        if isinstance(t_start, (int, float)):
            t_start = datetime.fromtimestamp(t_start)
        r["from"] = t_start.strftime("%Y%m%dT%H%M%S")
    if t_end:
        if isinstance(t_end, (int, float)):
            t_end = datetime.fromtimestamp(t_end)
        r["to"] = t_end.strftime("%Y%m%dT%H%M%S")
    r = {"range": r}
    r = await asks_session.post(url, data=json.dumps(dict(select=series, where=tags, **r)))

    if r.raw:
        if r.status_code != 200:
            raise RespError(r.reason_phrase, r.raw)
        br = Resp(BufferedReader(data=r.raw))
        while True:
            tags = await br.receive()  # ignore value
            if not tags:
                return
            tm = parse_timestamp(await br.receive())
            res = await br.receive()
            try:
                res = int(res)
            except ValueError:
                res = float(res)
            yield res, tm
    else:
        raise RuntimeError("Huh?", r)


def parse_timestamp(ts):
    """Parse ISO formatted timestamp"""
    try:
        return datetime.strptime(ts.rstrip("0").rstrip(".") + " +0000", "%Y%m%dT%H%M%S.%f %z")
    except ValueError:
        return datetime.strptime(ts.rstrip("0").rstrip(".") + " +0000", "%Y%m%dT%H%M%S %z")


def resp_encode(buf: List[bytes], data: ExtRespType):
    """
    Encode a message to ReSP.

    The message data is appended to the buffer list.
    """
    if isinstance(data, str) and "\r" not in data and "\n" not in data:
        buf.append(b"+" + str(data).encode("utf-8") + EOL)
    elif isinstance(data, int):
        buf.append(b":" + str(data).encode("ascii") + EOL)
    elif isinstance(data, float):
        buf.append(b"+" + str(data).encode("ascii") + EOL)
    elif isinstance(data, BaseException):
        buf.append(b"-" + str(data).encode("utf-8") + EOL)
    elif isinstance(data, (list, tuple)):
        buf.append(b"*" + str(len(data)).encode("ascii") + EOL)
        for d in data:
            resp_encode(buf, d)
    elif isinstance(data, bytes):
        buf.append("$" + str(len(data)).encode("ascii") + EOL)
        buf.append(data)
        buf.append(EOL)
    else:
        raise NoCodeError(data)


class Resp(AsyncResource):
    """
    This class implements Akumuli's RESP submission protocol.

    If "delta" is True, uses an EntryDelta filter to skip runs of
    consecutive values if appropriate.

    If "size" is set, queue submission is halted when more entries than
    that are queued.

    If "delay" is set, submission of entries is deferred until they're at
    least this #seconds old.
    """

    _t = None

    def __init__(self, stream, delay=0, size=0, delta=False):
        if not isinstance(stream, BufferedReader):
            stream = BufferedReader(stream)
        self.stream = stream
        self.buf = []
        self._dict = {}
        self._dt = 0
        self._heap = []
        self._same = EntryDelta() if delta else lambda x: x
        self._delay = delay
        self._heap_large: trio.Event = None  # waits for heap emptying
        self._heap_item: trio.Event = None  # waits for heappush
        self._done: trio.Event = None  # flush and shut down
        self._ending: trio.Event = None  # flush and shut down
        self._heap_max = size  # 100

    def preload(self, series: str, tags: RespTags):
        """
        Preload this series+tags entry to Akumuli
        """
        self._dt += 1
        self._dict.setdefault(series, {})[tags2str(tags)] = self._dt

    async def flush_dict(self):
        """
        Send the set of preload entries
        """
        di = []
        for k, v in self._dict.items():
            for kk, vv in v.items():
                di.append("%s %s" % (k, kk))
                di.append(vv)
        await self.send(di)

    async def aclose(self):
        await self.flush()
        await self.stream.aclose()

    async def send(self, data: ExtRespType):
        """Send this message."""
        resp_encode(self.buf, data)
        if len(self.buf) > 1000:
            await self.flush(heap=False)

    async def write(self, pkt: Entry):
        """Send the contents of this packet"""
        tags = pkt.tags_str
        try:
            dt = self._dict[pkt.series][tags]
        except KeyError:
            dt = "%s %s" % (pkt.series, tags)

        await self.send(dt)
        await self.send(pkt.ns_time)
        await self.send(pkt.value)

    async def put(self, pkt):
        """Store this packet, for eventual sending.
        """
        heapq.heappush(self._heap, pkt)
        if self._heap_item is not None:
            self._heap_item.set()
            self._heap_item = None

        if self._heap_max > 0:
            if self._heap_large is None and len(self._heap) >= self._heap_max:
                self._heap_large = trio.Event()
            if self._heap_large is not None:
                await self._heap_large.wait()

    async def _next_to_send(self):
        """
        Returns the next message on the heap
        """
        while True:
            while self._heap:
                if self._heap_large is not None and len(self._heap) < self._heap_max / 2:
                    self._heap_large.set()
                    self._heap_large = None

                if self._heap[0].time <= self._t - self._delay:
                    return heapq.heappop(self._heap)
                self._t = time.time()
                if self._heap[0].time <= self._t - self._delay:
                    return heapq.heappop(self._heap)
                with trio.move_on_after(max(self._delay + self._heap[0].time - self._t, 0)):
                    await self._ending.wait()

            if self._done is not None:
                self._done.set()
            self._heap_item = trio.Event()
            await self._heap_item.wait()

    async def send_all(self, task_status):
        """
        Send loop.
        """
        self._t = time.time()
        task_status.started()

        while True:
            e = await self._next_to_send()
            if e is None:
                return
            e = self._same(e)
            if e is not None:
                await self.write(e)

    async def flush(self, heap=True):
        """
        Send our write-buffered data.
        """
        if heap and self._heap:
            self._done = trio.Event()
            self._ending.set()
            await self._done.wait()
            self._ending = trio.Event()
            self._done = None

        buf = b"".join(self.buf)
        self.buf = []
        await self.stream.send_all(buf)

    async def receive(self):
        """
        Read a (possibly-complex) ReSP message.
        """
        try:
            line = await self.stream.readuntil(EOL)
        except IncompleteReadError as err:
            return err.chunk
        if not line:
            return None
        if isinstance(line, str):
            if line[0] == "+":
                return line[1 : -len(EOL)]
            elif line[0] == ":":
                return int(line[1 : -len(EOL)])
            elif line[0] == "-":
                raise RespError(line[1 : -len(EOL)], self.stream.read_buffer)
            elif line[0] == "*":
                n = int(line[1 : -len(EOL)])
                res = [self.receive() for _ in range(n)]
                return res
            # '$' not supported for char buffers
            else:
                raise RespUnknownError(line[: -len(EOL)])
        else:
            if line[0] == ord("+"):
                return line[1 : -len(EOL)].decode("utf-8")
            elif line[0] == ord(":"):
                return int(line[1 : -len(EOL)])
            elif line[0] == ord("-"):
                raise RespError(line[1 : -len(EOL)].decode("utf-8"), self.stream.read_buffer)
            elif line[0] == ord("*"):
                n = int(line[1 : -len(EOL)])
                res = []
                for _ in range(n):
                    res.append(await self.receive())
                return res
            elif line[0] == ord("$"):
                lb = int(line[1 : -len(EOL)])
                b = self.stream.readexactly(lb)
                eb = self.stream.readexactly(len(EOL))
                assert eb == EOL, eb
                return b
            else:
                raise RespUnknownError(line[: -len(EOL)])

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
async def connect(host="127.0.0.1", port=8282, **kw):
    """A context manager for managing the Resp object"""
    async with trio.open_nursery() as n:
        st = await trio.open_tcp_stream(host, port)
        s = Resp(st, **kw)
        await n.start(s.send_all)
        await n.start(reader, s)
        s._ending = trio.Event()
        try:
            yield s

        finally:
            with trio.move_on_after(5) as sc:
                sc.shield = True
                await s.flush()
                await trio.sleep(0.3)  # let's hope that's enough to read errors
            n.cancel_scope.cancel()
