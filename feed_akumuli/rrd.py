#!/usr/bin/python3

"""
Read RRD files from collectd
extract values
write to akumuli.
"""
from csv import writer
from lxml.etree import fromstring as parse_xml
from sys import argv, stdout
import os
import trio
from feed_akumuli.buffered import BufferedReader
from feed_akumuli.resp import Resp, connect as akumuli, RespError, parse_timestamp
import math
import asks
import json
import datetime

url="http://127.0.0.1:8181/api/query"
now = datetime.datetime.now().timestamp()

def set_series(datum):
    """convert a collectd/RRD data item to series+tags"""

    #time = 0
    #host = None
    #plugin = None
    #plugininstance = None
    #type = None
    #typeinstance = None

    #series
    #tags

    datum.series = None
    datum.tags = tags = {}
    datum.flags = flags = {}

    p = datum.plugin
    p_i = datum.plugininstance
    t = datum.type
    t_i = datum.typeinstance

    hn = datum.host.split('.')
    if False and hn[-3:] == ['smurf','noris','de']:
        del hn[-3:]
        hn.reverse()
        # s-r1 > r-a
        if len(hn) == 2 and hn[1][0] == 'r' and hn[0] == 's':
            hn[0] = 'r'
            hn[1] = "abcd"[int(hn[1][1:])-1]
        datum.host = '-'.join(hn)

    tags['typ'] = p
    if p_i is not None:
        tags['elem'] = p_i

    if p == "disk":
        # Ignore partitions, MD arrays, CD-ROMs, loopbacks, and whatnot.

        if p_i.startswith("sd") or p_i.startswith("vd"):
            if len(p_i) != 3:
                return
        elif p_i.startswith("cciss_c"):
            if len(p_i) != 8:
                return
        elif p_i.startswith("mtdblock"):
            if len(p_i) != 9:
                return
        elif p_i.startswith("mmcblk"):
            if len(p_i) != 7:
                return
        elif p_i.startswith("dm-"):
            return
        elif p_i.startswith("md"):
            return
        elif p_i.startswith("sr"):
            return
        elif p_i.startswith("loop"):
            return

        datum.series = "disk.load"
        t = t.split('_',1)
        tags['sub'] = t[0] if t[0] == "pending" else t[1]

    elif p == "interface":
        if p_i.startswith("eth") or p_i.startswith("en"):
            tags['medium'] = "wire"
        elif p_i.startswith("wl"):
            tags['medium'] = "air"
        elif p_i in {"netz","kabel","uplink","wan","dummy0","vppp"}:
            tags['medium'] = "dsl"
        elif '_' in p_i:
            d,m = p_i.split('_')
            tags['dest'] = d
            tags['medium'] = m
        elif p_i == "infra":
            tags['dest'] = "infra"
            tags['medium'] = "wire"
        elif p_i == "wire":
            tags['dest'] = "std"
            tags['medium'] = "wire"
        elif p_i == "draht":
            tags['dest'] = "std"
            tags['medium'] = "wire"
        elif p_i == "funk":
            tags['dest'] = "std"
            tags['medium'] = "air"
        elif p_i in {"wiresecure","vlan0042"}:
            tags['dest'] = "secure"
            tags['medium'] = "wire"
        elif p_i == "hanek":
            tags['dest'] = "guest"
            tags['medium'] = "hanek"
        elif p_i == "wireguest":
            tags['dest'] = "guest"
            tags['medium'] = "wire"
        elif p_i == "guest":
            tags['dest'] = "guest"
            tags['medium'] = "air"
        elif p_i == "lab":
            tags['dest'] = "lab"
            tags['medium'] = "wire"
        elif p_i == "init":
            tags['dest'] = "init"
            tags['medium'] = "wire"
        elif p_i == "backup":
            tags['dest'] = "backup"
            tags['medium'] = "wire"
        elif p_i in {"secure","lo","fups","erspan0"}:
            return
        elif p_i.startswith('bond'):
            return
        elif p_i.startswith('gre'):
            return
        elif p_i.startswith('rename'):
            return
        elif p_i.startswith('router'):
            return
        datum.series = "cpu.net"
        if t.startswith('if_'):
            t = t[3:]
        tags['sub'] = t

    elif p == "thermal":
        flags['zero_skip'] = True

        datum.series = "temp"
        tags['set'] = "cpu"
        if '0' <= p_i[-2] <= '9':
            tags['pos'] = p_i[-2:]
        else:
            tags['pos'] = p_i[-1]

    elif p == "df":
        if p_i.startswith('tmp-'):
            return
        if '-tmp-' in p_i:
            return
        if p_i.startswith('mnt-'):
            return
        if p_i.startswith('media-'):
            return
        if ' ' in p_i:
            return

        if t_i == "reserved":
            return
        datum.series = "disk.space"
        dx = p_i.rsplit('-',1)
        if len(dx) > 1:
            try:
                int(dx[-1])
            except ValueError:
                pass
            else:
                tags['elem'] = dx[0]+'-'
                tags['pos'] = dx[1]
        if t_i is not None:
            tags['sub'] = t_i

    elif p == "mysql":
        if t.startswith('mysql_'):
            t = t[6:]
            if t.startswith("sort_"):
                t_i = t[5:]
                t = "sort"
        elif t.startswith('cache_'):
            t = t[6:]
        elif t.startswith('total_'):
            t = t[6:]
        if t_i is not None and t_i.startswith("qcache-"):
            t_i = t_i[7:]
        if t in {"mysql","cache"}:
            tags['set'] = t
            if '_' in t_i:
                t,t_i = t_i.split('_',1)
            else:
                t,t_i = t_i,None
        datum.series = "mysql."+t
        if t_i is not None:
            tags['sub'] = t_i

    elif p == "chrony":
        tt = t.split('_',1)
        if tt[0] in {"clock","time","frequency"}:
            datum.series = "clock."+tt[1]
        else:
            datum.series = "clock."+t

        if t_i is None:
            t_i = "system"
        elif t_i == "chrony":
            t_i = "local"
        tags['remote'] = t_i

    elif p == "memory":
        datum.series = "cpu.mem"
        tags['sub'] = t_i

    elif p == "swap":
        datum.series = "cpu.swap.space"
        if t == "swap_io":
            datum.series = "cpu.swap.io"
        tags['sub'] = t_i

    elif p == "cpu":
        datum.series = "cpu.use"
        tags['sub'] = t_i

    elif p == "load":
        datum.series = "cpu.load"
        tags['sub'] = t

    elif p == "processes":
        t = t.split('_')
        if t[0] == "fork":
            datum.series = "cpu.load"
            tags['sub'] = "fork_rate"
        else:
            datum.series = "cpu.proc"
            tags['sub'] = t_i

    elif p == "irq":
        datum.series = 'cpu.irq'
        tags['sub'] = t_i

    else:
        return

    if datum.host == "store.intern.smurf.noris.de":
        datum.host = "store.s.smurf.noris.de"
    tags['host'] = datum.host

async def get_min_ts(q, datum):
    """
    Read the timestamp of the last entry.
    """
    r = await q.post(url, data=json.dumps(dict(aggregate={datum.series:"last"},
        where=datum.tags)))
    if r.raw:
        br = Resp(BufferedReader(data=r.raw))
        try:
            tag = await br.receive()
        except RespError as exc:
            if exc.args[0] == "not found":
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

async def push_one():

    async def push(path, min_ts, tree, series, kw, zero_skip=False):
        """
        Send this tree, starting with this timestamp, using this series+tags
        """
        tags = " ".join("%s=%s" % (k,v) for k,v in kw.items())
        #print("GO",path,series,tags)

        step = int(tree.find("step").text)
        ts = int(tree.find("lastupdate").text)
        if ts-ts%step <= min_ts:
            return

        # we're only interested in AVERAGE records
        rrs = []
        for r in tree.findall('rra'):
            if r.find('cf').text != "AVERAGE":
                continue
            rrs.append([r])
        if not rrs:
            print("NO AVG",path)
            return

        # add startdate and blocksize
        for i,r in enumerate(rrs):
            rr = r[0]
            pdp = int(rr.find('pdp_per_row').text)*step
            num = len(rr.find('database').findall('row'))
            r.append((num-1)*pdp) # start offset
            r.append(pdp) # step
        rrs.sort(key=lambda x: x[1])
        # Sorting by offset means the earlier records go last

        s=rr=off=pdp=num=skip=ls=None
        nc = 0
        async def push_():
            # The actual sender, encapsulated because it needs its own
            # nursery and akumuli connection
            nonlocal rr, off, pdp, num, skip, nc, s, ls

            await conn.send(["%s %s" % (series," ".join("%s=%s" % (k,v) for k,v in kw.items())), 1])
            if str(path) == "/mnt/c1/store.intern.smurf.noris.de/df-d-backup-my/df_complex-free.rrd":
                import pdb;pdb.set_trace()

            while rrs:
                # the earlier records are at the end, so …
                rr, off, pdp = rrs.pop()
                s = ts - ts%pdp - off

                # Calculate when we get to the next part
                if rrs:
                    _, off_, pdp_ = rrs[-1]
                    stop = ts - ts%pdp_ - off_
                else:
                    stop = math.inf
                if stop <= min_ts:
                    # no data here anyway
                    continue

                rall = rr.find("database").findall("row")
                # skip records in front?
                if s <= min_ts:
                    skip = int((min_ts-s) / pdp + 1)
                    s += skip*pdp
                    assert s > min_ts, (s,min_ts,skip,pdp)
                else:
                    skip=0

                for v in rall[skip:]:
                    if s >= stop:
                        break
                    v = float(v.find('v').text)
                    # Ignore some empty bits.
                    if math.isnan(v):
                        continue
                    if v == 0 and zero_skip:
                        continue
                    if ls is None or ls+0.5 < s:
                        # protect against nonsense
                        ls=s
                        await conn.send([1,int(s*1000000000),v], join=True)
                        nc += 1
                    s += pdp
        try:
            async with trio.open_nursery() as nn, \
                    akumuli(nn) as conn:
                try:
                    await push_()
                finally:
                    await conn.flush()
                    await trio.sleep(1) # wait for possible error
        except Exception as exc:
            print(f"*** ERROR *** {path} {series} {tags}")
            print(f"*** ERROR *** exc={exc} step={step} s={s} ls={ls} min={min_ts} pdp={pdp} num={num}")
        else:
            print("DID",nc,series,tags)

    async def process_one_(remote, path, kw):
        tags = {}
        flags = {}
        for k,v in kw.items():
            if k[0] == "_":
                flags[k[1:]] = v
            else:
                tags[k] = v
        if path.suffix != '.rrd':
            print("?",path)
            return
        dn = path.name[:-4].split("-")
        dn[0:1] = dn[0].split('_',1) 
        typ = tags.pop('typ',"_no_typ")


        try:
            async with cl2:
                min_ts = await get_min_ts(q, series, tags)
        except Exception:
            return
        s = await path.stat()
        if s.st_mtime+3600*24*7 < now:
            return

        async with cl1:
            tree = await read_rrd(remote, path)

        async with cl2:
            await push(path, min_ts, tree, series, tags, **flags)

    async def process_one(remote, path, kw):
        async with cl0:
            await process_one_(remote, path, kw)

    async def process_path(remote, path, depth, kw, skip=None):
        dn = os.path.basename(path)
        if skip is not None and dn == skip:
            return
        if depth == 1:
            if 'host' not in kw:
                kw['host'] = dn
        if await path.is_file():
            n.start_soon(process_one, remote, path, kw)

        elif await path.is_dir():
            depth += 1
            for subpath in await path.iterdir():
                n.start_soon(process_path, remote, subpath, depth, kw.copy(), skip)

    ## Work starts here
    
    q = asks.Session(connections=3)
    cl0 = trio.CapacityLimiter(20)  # RRDs in memory at the same time
    cl1 = trio.CapacityLimiter(3)  # RRS dump jobs
    cl2 = trio.CapacityLimiter(5)  # feed-to-Akumuli jobs

    async with trio.open_nursery() as n:
        # This host has been renamed
        await process_path("store.s.smurf.noris.de", trio.Path("/mnt/c1/store.intern.smurf.noris.de"), 1, {"host":"store.s.smurf.noris.de"})

    async with trio.open_nursery() as n:
        # Read from these subdirs, mounted from these hosts.
        await process_path("store.s.smurf.noris.de", trio.Path("/mnt/c1"), 0, {}, skip="store.intern.smurf.noris.de")
        await process_path("base.s.smurf.noris.de", trio.Path("/mnt/c2"), 0, {})

if __name__ == "__main__":
    trio.run(read_all)

