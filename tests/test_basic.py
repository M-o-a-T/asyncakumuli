from asyncakumuli.mock import Tester
from asyncakumuli import Entry, DS


async def test_basic():
    async with Tester().run() as tc:
        e = Entry(series="test-A", time=1500000000, value=123, tags={"foo": "one"}, mode=DS.gauge)
        await tc.put(e)
        await tc.flush()
        n = 0
        async for x in tc.get_data("test-A", tags={}, t_start=1490000000, t_end=1510000000):
            n += 1
            assert x[0] == 123
            assert x[1].timestamp() == 1500000000
        assert n == 1
