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

