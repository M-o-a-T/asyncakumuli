#!/usr/bin/python3

import trio
import trio_mysql as DB
import yaml
import trio_mysql.cursors
import sys
import httpx
import json
import datetime
from traceback import print_exc

from asyncakumuli import connect, get_min_ts
from asyncakumuli.buffered import BufferedReader
from asyncakumuli.model import Entry, DS, str2tags, tags2str

known = {}
special = {}

async def main():
    limits = httpx.Limits(max_keepalive_connections=1, max_connections=3)
    q = httpx.AsyncClient(timeout=600, limits=limits)
    cl = trio.CapacityLimiter(5)
    cff = open('cfg.yaml','r+')
    cfg = yaml.safe_load(cff)
    db_ = DB.connect(**cfg['mysql'], cursorclass=trio_mysql.cursors.SSDictCursor, init_command="set time_zone='+00:00';")

    async with trio.open_nursery() as N:
        async with db_ as db:
            async with db.cursor() as c:
                await c.execute("select * from data_type where series is NULL")
                async for r in c:
                    raise RuntimeError("not assigned", r)
                await c.execute("select id,series,tags,zero from data_type where series != '-'")
                async for r in c:
                    tags = str2tags(r['tags'])
                    known[r['id']] = (r['series'],tags)
                    if r['zero'] is not None:
                        special[r['id']] = [r['zero'], None]
            di = []
            async def one(dt):
                async with connect(N, host="stats.work.smurf.noris.de", delta=True) as s:
                    for v in known.values():
                        s.preload(v[0], v[1])
                    await s.flush_dict()

                    series,tags = known[dt]
                    async with cl:
                        ts = await get_min_ts(q, series, tags)
                        if ts < 0:
                            ts = t = None

                        sp = special.get(dt,None)
                        nn=0
                        n=0
                        while True:
                            nn+=n
                            n=0
                            async with db.cursor() as c:

                                ats = ""
                                if ts:
                                    ats = "and timestamp > from_unixtime(%s)" % (ts,)

                                await c.execute("select id, timestamp as t, unix_timestamp(timestamp) as ts, value from data_log where data_type = %s %s order by timestamp limit 10000" % (dt,ats))
                                async for r in c:
                                    e=Entry(value=r['value'],mode=DS.gauge,series=series,tags=tags,time=r['ts'])
                                    if sp:
                                        if sp[0] > 0:
                                            if sp[1] is not None:
                                                lts = sp[0]+sp[1]
                                                if lts < r['ts']:
                                                    ec=copy(e)
                                                    ec.value=0
                                                    ec.time=lts
                                                    await s.put(ec)
                                            sp[1] = r['ts']
                                        else:
                                            e.mode = DS.counter

                                    await s.put(e)
                                    n += 1
                                    t = r['t']
                                    ts = r['ts']
                                    cfg['at'] = {'t':t,'ts':ts,'dt':dt}
                            if not n:
                                break
                    print(dt,*known[dt],nn)

            async def one_(x):
                try:
                    await one(x)
                except Exception as exc:
                    print_exc()

            if len(sys.argv) < 2:
                for k in known.keys():
                    await one_(k)
            else:
                for k in sys.argv[1:]:
                    await one_(int(k))

            N.cancel_scope.cancel()


trio.run(main)
