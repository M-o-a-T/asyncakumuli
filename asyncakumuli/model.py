# data model

from enum import IntEnum
from typing import Dict
from datetime import datetime

__all__ = ["Entry", "DS"]


class DS(IntEnum):
    """short for DataSource"""

    invalid = -1
    counter = 0
    gauge = 1
    derive = 2
    absolute = 3
    delta = 4  # only skipped when zero, intended to be summed up


class Entry:
    """A data entry. It consists of a series, a number of tags, a time, and
    of course a value.
    """

    _tags: Dict[str, str] = None
    mode: DS = DS.invalid
    interval = None

    def __init__(self, value, time, series: str, tags: dict = None, mode=None):
        self.value = value
        self.time = time
        self.series = series
        self.tags = tags
        if mode is not None:
            self.mode = mode

    @property
    def tags(self):
        return self._tags

    @property
    def ns_time(self):
        t = self.time
        if isinstance(t, datetime):
            # way more accurate
            return int(t.timestamp()) * 1000000000 + t.microsecond * 1000
        else:
            return int(t * 1000000000)

    @tags.setter
    def tags(self, tags):
        if tags is None:
            tags = {}
        elif isinstance(tags, str):
            tags = str2tags(tags)
        self._tags = tags

    def __lt__(self, other):
        if self.time != other.time:
            return self.time < other.time
        if self.series != other.series:
            return self.series < other.series
        if self.tags != other.tags:
            return self.tags_str < other.tags_str
        return self.value < other.value

    def __eq__(self, other):
        if self.time != other.time:
            return False
        if self.series != other.series:
            return False
        if self.tags != other.tags:
            return False
        return True

    @property
    def key(self):
        """Key for hashing / """
        return (self.series, self.tags_str)

    @property
    def tags_str(self):
        return tags2str(self.tags)

    def __repr__(self):
        return "<%s:%s:%s %s %s %s>" % (
            self.__class__.__name__,
            self.mode,
            self.time,
            self.value,
            self.series,
            self.tags_str,
        )

    def __str__(self):
        return "%s %s@%s %s %s" % (self.value, self.mode, self.time, self.series, self.tags_str)


def str2tags(tags):
    if isinstance(tags, dict):
        return tags
    t = {}
    for kv in tags.split(" "):
        if not kv:
            continue
        k, v = kv.split("=", 1)
        try:
            v = int(v)
        except ValueError:
            try:
                v = float(v)
            except ValueError:
                pass
        t[k] = v
    return t


def tags2str(tags):
    return " ".join("%s=%s" % (k, v) for k, v in sorted(tags.items()))


class EntryDelta:
    """
    This class accepts an entry which it may or may not return later.

    All non-raw data you feed to this are converted to gauges, i.e. rate
    per second.

    Use::
        f = EntryDelta()
        async for e in read_entries():
            e = f(e)
            if e is None:
                continue
            await store_entry(e)
    """

    def __init__(self):
        self._last = {}
        self._prev = {}

    def __call__(self, entry):
        k = entry.key
        if entry.mode == DS.invalid:
            raise RuntimeError("mode needs to be set")
        if entry.mode in {DS.counter, DS.derive, DS.absolute}:
            # First step: calculate delta.
            v = entry.value
            try:
                ov, ot = self._last[k]
            except KeyError:
                self._last[k] = (entry.value, entry.time)
                return
            else:
                t = entry.time
            if ot >= t:
                return
            r = t - ot
            self._last[k] = (entry.value, t)

            if entry.mode == DS.derive:
                entry.value = (v - ov) / r
            elif entry.mode == DS.counter:
                if v < ov:
                    if ov < 2 ** 32:
                        v += 2 ** 32
                    else:
                        v += 2 ** 64
                entry.value = (v - ov) / r
            elif entry.mode == DS.absolute:
                entry.value /= r

            # We have successfully removed the time based element.

        # Second step: Return the previous value if there either is or was
        # a difference. Invariant: an entry stored in self._prev has a
        # "__dup" attribute which is True iff the entry before that, already
        # stored in the database, has the same value.
        try:
            prev = self._prev[k]
        except KeyError:
            entry.__dup = False
            return None
        else:
            if prev.value == entry.value and (entry.mode != DS.delta or entry.value == 0):
                # We cannot skip identical Delta values unless they're zero.
                entry.__dup = True
                if prev.__dup:
                    return None
            else:
                entry.__dup = False
            if prev.mode < DS.delta:
                prev.mode = DS.gauge
            return prev
        finally:
            self._prev[k] = entry

    def flush(self):
        """Iterate stored entries."""
        for k, v in self._prev.items():
            try:
                lv, lt = self._last[k]
            except KeyError:
                pass
            else:
                v.abs_value = lv
                v.time = lt
            yield v
        self._prev = {}

    def prep(self, entry):
        """Restore saved entry from previous run.
        Requires "abs_value" to be restored!
        """
        k = entry.key
        if k in self._prev:
            raise RuntimeError("known", k)
        entry.__dup = False
        self._prev[k] = entry
        if entry.mode in {DS.counter, DS.derive, DS.absolute}:
            self._last = (entry.abs_value, entry.time)
