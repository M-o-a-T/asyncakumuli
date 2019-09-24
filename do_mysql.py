#!/usr/bin/python3

import trio
import trio_mysql as DB
import yaml
import trio_mysql.cursors
import sys
import asks
import json
import datetime

from feed_akumuli.resp import Resp, parse_timestamp, get_min_ts
from feed_akumuli.buffered import BufferedReader

async def reader(s, task_status=None):
    task_status.started()
    async for m in s:
        raise RuntimeError(m)

known = {}
special = {}

async def main():
    q = asks.Session(connections=3)
    cl = trio.CapacityLimiter(5)
    cff = open('cfg.yaml','r+')
    cfg = yaml.safe_load(cff)
    db_ = DB.connect(**cfg['mysql'], cursorclass=trio_mysql.cursors.SSDictCursor, init_command="set time_zone='+00:00';")

    async with trio.open_nursery() as N:
        async with db_ as db:
            async with await trio.open_tcp_stream("localhost",8282) as s:
                s = Resp(s)
                await N.start(reader, s)
                async with db.cursor() as c:
                    await c.execute("select * from data_type where series is NULL")
                    async for r in c:
                        raise RuntimeError("not assigned", r)
                    await c.execute("select id,series,tags,zero from data_type where series != '-'")
                    async for r in c:
                        known[r['id']] = (r['series'],r['tags'])
                        if r['zero'] is not None:
                            special[r['id']] = [r['zero'], None]
                di = []
                for k,v in known.items():
                    s.preload(k,v)
                await s.flush_dict()

                async def one(dt):
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
                                if t:
                                    ats = "and timestamp > '%s'" % (t,)

                                await c.execute("select id, timestamp as t, unix_timestamp(timestamp) as ts, value from data_log where data_type = %s %s order by timestamp limit 10000" % (dt,ats))
                                async for r in c:
                                    val = r['value']
                                    if sp:
                                        if sp[0] > 0:
                                            if sp[1] is not None:
                                                lts = sp[0]+sp[1]
                                                if lts < r['ts']:
                                                    await s.write(series,tags,r['ts'], 0)
                                            sp[1] = r['ts']
                                        else:
                                            import pdb;pdb.set_trace()
                                            ov = val
                                            if sp[1] is None:
                                                val = None
                                            elif val >= sp[1]:
                                                val -= sp[1]
                                            sp[1] = ov

                                    if val is not None:
                                        await s.write(series,tags,r['ts'],val)
                                    n += 1
                                    t = r['t']
                                    ts = r['ts']
                                    cfg['at'] = {'t':t,'ts':ts,'dt':dt}
                            await s.flush()
                            if not n:
                                break
                        print(dt,*known[dt],nn)

                if len(sys.argv) < 2:
                    for k in known.keys():
                        await one(k)
                else:
                    for k in sys.argv[1:]:
                        await one(int(k))

        N.cancel_scope.cancel()


trio.run(main)
