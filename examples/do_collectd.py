#!/usr/bin/python3

"""
Read RRD files from collectd
extract values
write to akumuli.
"""
from csv import writer
from lxml.etree import fromstring as parse_xml
from sys import argv, stdout
import sys
import os
import trio
import math
import httpx
import json
import shlex
import datetime
from pprint import pprint
from copy import deepcopy

from asyncakumuli.collectd import Data, Reader
from asyncakumuli import connect as akumuli, get_min_ts, EntryDelta

url="http://127.0.0.1:8181/api/query"
now = datetime.datetime.now().timestamp()

async def read_all():
    """
    The main processor. Sub-functions do the actual work.
    """
    deriv = {}
    same = {}

    delta = EntryDelta()

    async with Reader.new(host="2001:780:107::15") as r:
        async for pkt in r:
            if not pkt.set_series():
                continue
            pkt = delta(pkt)
            if pkt is None:
                continue
            print(pkt)
    return

    async def read_rrd(remote,fn):
        """
        Dump a RRD file to XML and parse it.
        """
        if remote:
            # This is a hack
            fn = "/var/lib/rrdcached/db/collectd/"+str(fn)[7:]
            # ssh is broken WRT quoting
            p = await trio.run_process(["ssh","-n",remote,"rrdtool","dump",shlex.quote(fn)], capture_stdout=True)
        else:
            p = await trio.run_process(["rrdtool","dump",str(fn)], capture_stdout=True)
        # Moving the parser to a background task doesn't pseed things up
        return parse_xml(p.stdout)

    async def push(path, min_ts, tree, datum, zero_skip=False):
        """
        Send this tree, starting with this timestamp, using this series+tags
        """
        tags = " ".join("%s=%s" % (k,v) for k,v in datum.tags.items())
        #print("GO",path,datum.series,tags)

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

            await conn.send(["%s %s" % (datum.series,tags), 1])

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
            print(f"*** ERROR *** {path} {datum.series} {tags}")
            print(f"*** ERROR *** exc={exc} step={step} s={s} ls={ls} min={min_ts} pdp={pdp} num={num}")
        else:
            print("DID",nc,datum.series,tags)

    async def process_one_(remote, path, datum):
        tags = datum.tags
        flags = datum.flags
        async with cl2:
            min_ts = await get_min_ts(q, datum.series, datum.tags)

        s = await path.stat()
        if s.st_mtime+3600*24*7 < now:
            return

        async with cl1:
            tree = await read_rrd(remote, path)

        async with cl2:
            await push(path, min_ts, tree, datum, **flags)
        return

    async def process_one(remote, path, datum):
        if path.suffix != '.rrd':
            print("?",path)
            return
        dn = path.name[:-4].split("-")
        datum.type = dn[0]
        datum.typeinstance = '-'.join(dn[1:]) if len(dn)>1 else None

        if not set_series(datum):
            return
        async with cl0:
            try:
                await process_one_(remote, path, datum)
            except Exception as e:
                print("Unable to process", path, file=sys.stderr)
                raise

    async def process_path(remote, path, depth, datum, skip=None):
        dn = os.path.basename(path)
        if skip is not None and dn == skip:
            return

        if depth == 1:
            datum.host = dn
        elif depth == 2:
            dn = dn.split('-',1)
            datum.plugin = dn[0]
            datum.plugininstance = dn[1] if len(dn)>1 else None

        if await path.is_file():
            n.start_soon(process_one, remote, path, datum)

        elif await path.is_dir():
            depth += 1
            for subpath in await path.iterdir():
                n.start_soon(process_path, remote, subpath, depth, deepcopy(datum), skip)

    ## Work starts here
    
    limits = httpx.Limits(max_keepalive_connections=1, max_connections=3)
    q = httpx.AsyncClient(timeout=600, limits=limits)
    cl0 = trio.CapacityLimiter(20)  # RRDs in memory at the same time
    cl1 = trio.CapacityLimiter(3)  # RRS dump jobs
    cl2 = trio.CapacityLimiter(5)  # feed-to-Akumuli jobs

    async with trio.open_nursery() as n:
        # This host has been renamed
        await process_path("store.s.smurf.noris.de", trio.Path("/mnt/c1/store.intern.smurf.noris.de"), 1, Data())

    async with trio.open_nursery() as n:
        # Read from these subdirs, mounted from these hosts.
        await process_path("store.s.smurf.noris.de", trio.Path("/mnt/c1"), 0, Data(), skip="store.intern.smurf.noris.de")
        await process_path("base.s.smurf.noris.de", trio.Path("/mnt/c2"), 0, Data())

if __name__ == "__main__":
    trio.run(read_all)

