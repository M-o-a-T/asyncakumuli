#!/usr/bin/python3

"""
This sample program feeds the current German spot market prices for
electricity into Akumuli.

"""

from datetime import datetime,timedelta,timezone
from asyncakumuli import connect, Entry, DS, get_max_ts
import httpx
from pprint import pprint
import trio
tz = datetime.now(timezone(timedelta(0))).astimezone().tzinfo

url = "https://api.awattar.at/v1/marketdata"
start = datetime(2020,1,1).astimezone(tz)
#start = datetime.now().astimezone(tz)
end = datetime.now().astimezone(tz)+timedelta(days=7)
print(end)
end = end.replace(hour=0,minute=0,second=0)
mode=DS.gauge
series="price"
tags=dict(type="power",source="awattar")

async def main(start):
	limits = httpx.Limits(max_keepalive_connections=1, max_connections=3)
	async with httpx.AsyncClient(timeout=600, limits=limits) as s, \
			connect("a.rock.s") as ak:
		q = await get_max_ts(s, series, tags, url = "http://a.rock.s:8181/api/query")
		if q:
			start = datetime.fromtimestamp(q,tz=tz)+timedelta(hours=1)
			print(start,int(start.timestamp())*1000)

		while start < end:
			e = start+timedelta(days=7)
			r = await s.get(url,params=dict(start=int(start.timestamp())*1000,
				end=int(min(e,end).timestamp())*1000))
			start=e
			dd = r.json()["data"]
			if not dd:
				break
			for d in dd:
				p=d["marketprice"]
				st=d["start_timestamp"]//1000
				e = Entry(series=series, mode=mode, time=st*1000000000, value=p, tags=tags)
				await ak.write(e)
				print(e)
				await ak.flush_buf()

trio.run(main,start)

