import datetime
from contextlib import aclosing

import anyio
from pytz import UTC

from asyncakumuli import DS, Entry
from asyncakumuli.mock import Tester


async def test_basic():
    async with Tester().run() as tc:
        e = Entry(
            series="test-A",
            time=1500000000,
            value=123,
            tags={"foo": "one"},
            mode=DS.gauge,
        )
        await tc.put(e)
        await anyio.sleep(0.1)
        n = 0
        gd = tc.get_data("test-A", tags={}, t_start=1490000000, t_end=1510000000)
        async with aclosing(gd):
            async for x in gd:
                n += 1
                assert x.value == 123
                assert x.time == 1500000000
                assert x.date == datetime.datetime(2017, 7, 14, 2, 40, tzinfo=UTC)
        assert n == 1
