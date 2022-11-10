import os
import tempfile
from contextlib import asynccontextmanager

import asks
import trio

from asyncakumuli import connect
from asyncakumuli import get_data as _get_data
from asyncakumuli import get_max_ts as _get_max_ts

TCP_PORT = (os.getpid() + 23) % 10000 + 40000
HTTP_PORT = (os.getpid() + 24) % 10000 + 40000
URL = f"http://localhost:{HTTP_PORT}/api/query"


class Tester:
    _client = None
    _server = None
    _session = None

    @staticmethod
    @asynccontextmanager
    async def _daemon(http=HTTP_PORT, tcp=TCP_PORT):
        async with trio.open_nursery() as n:
            with tempfile.TemporaryDirectory() as d:
                cfg = os.path.join(d, "test.cfg")
                with open(cfg, "w", encoding="utf-8") as f:
                    print(
                        f"""\
path={d}
nvolumes=0
volume_size=2MB
[HTTP]
# port number
port={http}
[TCP]
port={tcp}
pool_size=0

log4j.rootLogger=info, file
log4j.appender.file=org.apache.log4j.DailyRollingFileAppender
log4j.appender.file.layout=org.apache.log4j.PatternLayout
log4j.appender.file.layout.ConversionPattern=%d{{yyyy-MM-dd HH:mm:ss,SSS}} [%t] %c [%p] %m%n
log4j.appender.file.filename={d}/akumuli.log
log4j.appender.file.datePattern='.'yyyy-MM-dd

""",
                        file=f,
                    )
                print(d)
                proc = await trio.run_process(
                    ["akumulid", "--create", "--allocate", "--config", cfg]
                )
                proc = await n.start(trio.run_process, ["akumulid", "--config", cfg])
                try:
                    with trio.fail_after(10):
                        while True:
                            try:
                                s = await trio.open_tcp_stream("127.0.0.1", TCP_PORT)
                                await s.aclose()
                                break
                            except OSError:
                                await trio.sleep(0.1)
                    yield proc
                finally:
                    proc.terminate()
                    with trio.move_on_after(2) as cs:
                        cs.shield = True
                        await proc.wait()
                    if proc.poll() is None:
                        proc.kill()

    @asynccontextmanager
    async def run(self):
        async with self._daemon() as server, asks.Session() as session, connect(
            port=TCP_PORT
        ) as client:
            self._server = server
            self._session = session
            self._client = client
            yield self

    @property
    def port(self):
        return self._client

    def get_data(self, *a, **k):
        k.setdefault("url", URL)
        return _get_data(self._session, *a, **k)

    def get_max_ts(self, *a, **k):
        k.setdefault("url", URL)
        return _get_max_ts(self._session, *a, **k)

    def preload(self, *a, **k):
        return self._client.preload(*a, **k)

    def flush_dict(self, *a, **k):
        return self._client.flush_dict(*a, **k)

    def put(self, *a, **k):
        return self._client.put(*a, **k)

    def flush(self, *a, **k):
        return self._client.flush(*a, **k)
