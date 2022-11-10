#!/usr/bin/python3
# -*- coding: utf-8 -*-
# vim: fileencoding=utf-8
#
# Copyright © 2009 Adrian Perez <aperez@igalia.com>
# Copyright © 2019 Matthias Urlichs <matthias@urlichs.de>
#
# Distributed under terms of the GPLv2 license.

"""
Collectd network protocol implementation,
forwarding to akumuli.
"""

import re
import struct
from contextlib import asynccontextmanager
from copy import deepcopy
from enum import IntEnum

import trio
from trio import socket

from .model import DS, Entry

DEFAULT_PORT = 25826
"""Default port"""

DEFAULT_IPv4_GROUP = "239.192.74.66"
"""Default IPv4 multicast group"""

DEFAULT_IPv6_GROUP = "ff18::efc0:4a42"
"""Default IPv6 multicast group"""


# Message kinds
class TYPE(IntEnum):
    HOST = 0x0000
    TIME = 0x0001
    PLUGIN = 0x0002
    PLUGIN_INSTANCE = 0x0003
    TYPE = 0x0004
    TYPE_INSTANCE = 0x0005
    VALUES = 0x0006
    INTERVAL = 0x0007
    TIME_HIRES = 0x0008
    INTERVAL_HIRES = 0x0009

    # For notifications
    MESSAGE = 0x0100
    SEVERITY = 0x0101


header = struct.Struct("!2H")
number = struct.Struct("!Q")
number_s = struct.Struct("!q")
short = struct.Struct("!H")
double = struct.Struct("<d")


def decode_network_values(ptype, plen, buf):  # pylint: disable=unused-argument
    """Decodes a list of DS values in collectd network format"""
    nvalues = short.unpack_from(buf, header.size)[0]
    off = header.size + short.size + nvalues
    valskip = double.size

    # Check whether our expected packet size is the reported one
    assert ((valskip + 1) * nvalues + short.size + header.size) == plen
    assert double.size == number.size

    result = []
    for dstype in buf[header.size + short.size : off]:
        if dstype == DS.counter:
            result.append((dstype, number.unpack_from(buf, off)[0]))
        elif dstype == DS.gauge:
            result.append((dstype, double.unpack_from(buf, off)[0]))
        elif dstype == DS.derive:
            result.append((dstype, number_s.unpack_from(buf, off)[0]))
        elif dstype == DS.absolute:
            result.append((dstype, number.unpack_from(buf, off)[0]))
        else:
            raise ValueError("DS type %i unsupported" % dstype)
        off += valskip

    return result


HIRES_SCALE = 2**30


def decode_network_number(ptype, plen, buf):  # pylint: disable=unused-argument
    """Decodes a number (64-bit unsigned) in collectd network format."""
    return number.unpack_from(buf, header.size)[0]


def decode_network_number_hr(ptype, plen, buf):  # pylint: disable=unused-argument
    """Decodes a number (64-bit unsigned, accurracy 2^-30) in collectd network format."""
    return number.unpack_from(buf, header.size)[0] / HIRES_SCALE


def decode_network_string(msgtype, plen, buf):  # pylint: disable=unused-argument
    """Decodes a floating point number (64-bit) in collectd network format."""
    return buf[header.size : plen - 1].decode("utf-8")


# Mapping of message types to decoding functions.
_decoders = {
    TYPE.VALUES: decode_network_values,
    TYPE.TIME: decode_network_number,
    TYPE.TIME_HIRES: decode_network_number_hr,
    TYPE.INTERVAL: decode_network_number,
    TYPE.INTERVAL_HIRES: decode_network_number_hr,
    TYPE.HOST: decode_network_string,
    TYPE.PLUGIN: decode_network_string,
    TYPE.PLUGIN_INSTANCE: decode_network_string,
    TYPE.TYPE: decode_network_string,
    TYPE.TYPE_INSTANCE: decode_network_string,
    TYPE.MESSAGE: decode_network_string,
    TYPE.SEVERITY: decode_network_number,
}


def decode_network_packet(buf):
    """Decodes a network packet in collectd format."""
    off = 0
    blen = len(buf)
    while off < blen:
        ptype, plen = header.unpack_from(buf, off)

        if plen > blen - off:
            raise ValueError("Packet longer than amount of data in buffer")

        if ptype not in _decoders:
            raise ValueError("Message type %i not recognized" % ptype)

        yield ptype, _decoders[ptype](ptype, plen, buf[off:])
        off += plen


class Data(Entry):
    time = 0
    host = None
    plugin = None
    plugininstance = None
    type = None
    typeinstance = None

    def __init__(self, **kw):  # pylint: disable=super-init-not-called
        # yes I know that super-init-not-called is dangerous
        for k, v in kw.items():
            setattr(self, k, v)

    #   @property
    #   def datetime(self):
    #       return datetime.fromtimestamp(self.time)

    @property
    def source(self):
        buf = []
        if self.host:
            buf.append(self.host)
        if self.plugin:
            buf.append(self.plugin)
        if self.plugininstance:
            buf.append(self.plugininstance)
        if self.type:
            buf.append(self.type)
        if self.typeinstance:
            buf.append(self.typeinstance)
        if hasattr(self, "value"):
            buf.append("=" + str(self.value))
        return "/".join(buf)

    def __str__(self):
        return "[%i] %s" % (self.time, self.source)

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, str(self))


class Value(Data, Entry):
    @staticmethod
    def set_series(datum):
        """convert a collectd/RRD data item to series+tags.

        Returns True if OK.
        """

        # time = 0
        # host = None
        # plugin = None
        # plugininstance = None
        # type = None
        # typeinstance = None

        # series
        # tags

        datum.series = None
        datum.tags = tags = {}
        datum.flags = {}

        p = datum.plugin
        p_i = datum.plugininstance
        t = datum.type
        t_i = datum.typeinstance

        hn = datum.host.split(".")
        if hn[-3:] == ["smurf", "noris", "de"]:
            del hn[-3:]
            hn.reverse()
            # s-r1 > r-a
            if len(hn) == 2 and hn[1][0] == "r" and hn[0] == "s":
                hn[0] = "r"
                hn[1] = "abcd"[int(hn[1][1:]) - 1]
            datum.host = "-".join(hn)

        tags["typ"] = p
        if p_i is not None:
            tags["elem"] = p_i

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
            t = t.split("_", 1)
            tags["sub"] = t[0] if t[0] == "pending" else t[1]
            if t_i is not None:
                tags["dir"] = t_i

        elif p == "interface":
            if p_i.startswith("eth") or p_i.startswith("en"):
                tags["medium"] = "wire"
            elif p_i.startswith("wl"):
                tags["medium"] = "air"
            elif p_i in {"netz", "kabel", "uplink", "wan", "dummy0", "vppp"}:
                tags["medium"] = "dsl"
            elif "_" in p_i:
                d, m = p_i.split("_")
                tags["dest"] = d
                tags["medium"] = m
            elif p_i == "infra":
                tags["dest"] = "infra"
                tags["medium"] = "wire"
            elif p_i == "wire":
                tags["dest"] = "std"
                tags["medium"] = "wire"
            elif p_i == "draht":
                tags["dest"] = "std"
                tags["medium"] = "wire"
            elif p_i == "funk":
                tags["dest"] = "std"
                tags["medium"] = "air"
            elif p_i in {"wiresecure", "vlan0042"}:
                tags["dest"] = "secure"
                tags["medium"] = "wire"
            elif p_i == "hanek":
                tags["dest"] = "guest"
                tags["medium"] = "hanek"
            elif p_i == "wireguest":
                tags["dest"] = "guest"
                tags["medium"] = "wire"
            elif p_i == "guest":
                tags["dest"] = "guest"
                tags["medium"] = "air"
            elif p_i == "lab":
                tags["dest"] = "lab"
                tags["medium"] = "wire"
            elif p_i == "init":
                tags["dest"] = "init"
                tags["medium"] = "wire"
            elif p_i == "backup":
                tags["dest"] = "backup"
                tags["medium"] = "wire"
            elif p_i in {"secure", "lo", "fups", "erspan0"}:
                return
            elif p_i.startswith("bond"):
                return
            elif p_i.startswith("gre"):
                return
            elif p_i.startswith("rename"):
                return
            elif p_i.startswith("router"):
                return
            datum.series = "cpu.net"
            if t.startswith("if_"):
                t = t[3:]
            tags["sub"] = t
            if t_i is not None:
                tags["dir"] = t_i

        elif p == "thermal":
            datum.series = "temp"
            tags["set"] = "cpu"
            if "0" <= p_i[-2] <= "9":
                tags["pos"] = p_i[-2:]
            else:
                tags["pos"] = p_i[-1]

        elif p == "df":
            if p_i.startswith("tmp-"):
                return
            if "-tmp-" in p_i:
                return
            if p_i.startswith("mnt-"):
                return
            if p_i.startswith("media-"):
                return
            if " " in p_i:
                return

            if t_i == "reserved":
                return
            datum.series = "disk.space"
            dx = p_i.rsplit("-", 1)
            if len(dx) > 1:
                try:
                    int(dx[-1])
                except ValueError:
                    pass
                else:
                    tags["elem"] = dx[0] + "-"
                    tags["pos"] = dx[1]
            if t_i is not None:
                tags["sub"] = t_i

        elif p == "mysql":
            if t.startswith("mysql_"):
                t = t[6:]
                if t.startswith("sort_"):
                    t_i = t[5:]
                    t = "sort"
            elif t.startswith("cache_"):
                t = t[6:]
            elif t.startswith("total_"):
                t = t[6:]
            if t_i is not None and t_i.startswith("qcache-"):
                t_i = t_i[7:]
            if t in {"mysql", "cache"}:
                tags["set"] = t
                if "_" in t_i:
                    t, t_i = t_i.split("_", 1)
                else:
                    t, t_i = t_i, None
            datum.series = "mysql." + t
            if t_i is not None:
                tags["sub"] = t_i

        elif p == "chrony":
            tt = t.split("_", 1)
            if tt[0] in {"clock", "time", "frequency"}:
                datum.series = "clock." + tt[1]
            else:
                datum.series = "clock." + t

            if t_i is None:
                t_i = "system"
            elif t_i == "chrony":
                t_i = "local"
            tags["remote"] = t_i

        elif p == "memory":
            datum.series = "cpu.mem"
            tags["sub"] = t_i

        elif p == "swap":
            datum.series = "cpu.swap.space"
            if t == "swap_io":
                datum.series = "cpu.swap.io"
            tags["sub"] = t_i

        elif p == "cpu":
            datum.series = "cpu.use"
            tags["sub"] = t_i

        elif p == "load":
            datum.series = "cpu.load"
            tags["sub"] = t

        elif p == "processes":
            t = t.split("_")
            if t[0] == "fork":
                datum.series = "cpu.load"
                tags["sub"] = "fork_rate"
            else:
                datum.series = "cpu.proc"
                tags["sub"] = t_i

        elif p == "irq":
            datum.series = "cpu.irq"
            tags["sub"] = t_i

        if datum.series is None:
            return False

        if datum.host == "store.intern.smurf.noris.de":
            datum.host = "store.s.smurf.noris.de"
        tags["host"] = datum.host
        return True


class Notification(Data):
    FAILURE = 1
    WARNING = 2
    OKAY = 4

    SEVERITY = {
        FAILURE: "FAILURE",
        WARNING: "WARNING",
        OKAY: "OKAY",
    }

    __severity = 0
    message = ""

    def __set_severity(self, value):
        if value in (self.FAILURE, self.WARNING, self.OKAY):
            self.__severity = value

    severity = property(lambda self: self.__severity, __set_severity)

    @property
    def severitystring(self):
        return self.SEVERITY.get(self.severity, "UNKNOWN")

    def __str__(self):
        return "%s [%s] %s" % (
            super(Notification, self).__str__(),
            self.severitystring,
            self.message,
        )


class Reader:
    """Network reader for collectd data.

    Listens on the network in a given address, which can be a multicast
    group address, and handles reading data when it arrives.
    """

    addr = None
    host = None
    port = DEFAULT_PORT
    _sock = None
    _it = None

    BUFFER_SIZE = 10240

    def __init__(
        self,
        host=None,
        db="/usr/share/collectd/types.db",
        port=DEFAULT_PORT,
        multicast=False,
    ):
        if host is None:
            multicast = True
            host = DEFAULT_IPv4_GROUP

        self.host, self.port = host, port
        self.ipv6 = ":" in self.host
        self.multicast = multicast
        self.db = db

    @classmethod
    def new(cls, *a, **k):
        s = cls(*a, **k)
        return s._task()

    async def _read_db(self):
        db = {}
        multi_pat = re.compile(r"^([a-z]\S*)\s+(\S+, *.*\S)\s*$")
        split_pat = re.compile(r",\s*")

        async with await trio.Path(self.db).open("r") as p:
            async for line in p:
                names = []
                m = multi_pat.match(line)
                if m is None:
                    continue
                for t in split_pat.split(m.group(2)):
                    t = t.split(":", 1)
                    names.append(t[0])
                db[m.group(1)] = names
        self.db = db

    @asynccontextmanager
    async def _task(self):
        try:
            await self._read_db()
            await self._init()
            yield self
        finally:
            if self._sock is not None:
                await self._sock.aclose()

    async def _init(self):
        family, socktype, proto, _canonname, sockaddr = (
            await socket.getaddrinfo(
                None if self.multicast else self.host,
                self.port,
                socket.AF_INET6 if self.ipv6 else socket.AF_UNSPEC,
                socket.SOCK_DGRAM,
                0,
                socket.AI_PASSIVE,
            )
        )[0]

        self._sock = socket.socket(family, socktype, proto)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        await self._sock.bind(sockaddr)

        if self.multicast:
            if hasattr(socket, "SO_REUSEPORT"):
                self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

            val = None
            if family == socket.AF_INET:
                assert "." in self.host
                val = struct.pack("4sl", socket.inet_aton(self.host), socket.INADDR_ANY)
            elif family == socket.AF_INET6:
                raise NotImplementedError("IPv6 support not ready yet")
            else:
                raise ValueError("Unsupported network address family")

            self._sock.setsockopt(
                socket.IPPROTO_IPV6 if self.ipv6 else socket.IPPROTO_IP,
                socket.IP_ADD_MEMBERSHIP,
                val,
            )
            self._sock.setsockopt(
                socket.IPPROTO_IPV6 if self.ipv6 else socket.IPPROTO_IP,
                socket.IP_MULTICAST_LOOP,
                0,
            )

    async def receive(self):
        """Receives a single raw collect network packet."""
        return await self._sock.recv(self.BUFFER_SIZE)

    async def decode(self, buf=None):
        """Decodes a given buffer or the next received packet."""
        if buf is None:
            buf = await self.receive()
        return decode_network_packet(buf)

    def interpret_opcodes(self, iterable):
        vl = Value()
        nt = Notification()

        for kind, data in iterable:
            if kind in {TYPE.TIME, TYPE.TIME_HIRES}:
                vl.time = nt.time = data
            elif kind in {TYPE.INTERVAL, TYPE.INTERVAL_HIRES}:
                vl.interval = data
            elif kind == TYPE.HOST:
                vl.host = nt.host = data
            elif kind == TYPE.PLUGIN:
                vl.plugin = nt.plugin = data
            elif kind == TYPE.PLUGIN_INSTANCE:
                vl.plugininstance = nt.plugininstance = data
            elif kind == TYPE.TYPE:
                vl.type = nt.type = data
            elif kind == TYPE.TYPE_INSTANCE:
                vl.typeinstance = nt.typeinstance = data
            elif kind == TYPE.SEVERITY:
                nt.severity = data
            elif kind == TYPE.MESSAGE:
                nt.message = data
                yield deepcopy(nt)
            elif kind == TYPE.VALUES:
                if len(data) > 1:
                    instances = self.db[vl.type]
                    for t, d in zip(instances, data):
                        vl.typeinstance = t
                        vl.mode = d[0]
                        vl.value = d[1]
                        yield deepcopy(vl)
                    vl.typeinstance = None
                else:
                    vl.mode = data[0][0]
                    vl.value = data[0][1]
                    yield deepcopy(vl)

    async def interpret(self, iterable=None):
        """Interprets a sequence"""
        if iterable is None:
            iterable = await self.decode()
        if isinstance(iterable, str):
            iterable = self.decode(iterable)
        return self.interpret_opcodes(iterable)

    def __aiter__(self):
        self._it = None
        return self

    async def __anext__(self):
        while True:
            if self._it is None:
                pkt = await self.receive()
                dec_it = decode_network_packet(pkt)
                self._it = self.interpret_opcodes(dec_it)
            try:
                pkt = next(self._it)
            except StopIteration:
                self._it = None
                pass
            else:
                return pkt
