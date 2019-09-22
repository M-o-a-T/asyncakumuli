#!/usr/bin/python3

import trio
import trio_mysql as DB
import yaml
import trio_mysql.cursors
import sys
import asks
import json
import datetime

from db2aku.resp import Resp
from db2aku.buffered import BufferedReader

async def reader(s, task_status=None):
	task_status.started()
	async for m in s:
		raise RuntimeError(m)

known = {}
special = {}

url="http://127.0.0.1:8181/api/query"

def parse_timestamp(ts):
    """Parse ISO formatted timestamp"""
    try:
        return datetime.datetime.strptime(ts.rstrip('0').rstrip('.')+" +0000", "%Y%m%dT%H%M%S.%f %z")
    except ValueError:
        return datetime.datetime.strptime(ts.rstrip('0').rstrip('.')+" +0000", "%Y%m%dT%H%M%S %z")

async def main():
	q = asks.Session(connections=3)
	cl = trio.CapacityLimiter(5)
	cff = open('cfg.yaml','r+')
	cfg = yaml.safe_load(cff)
	db_ = DB.connect(**cfg['db'], cursorclass=trio_mysql.cursors.SSDictCursor, init_command="set time_zone='+00:00';")
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
					di.append(" ".join(v))
					di.append(k)
				await s.send(di)

				async def one(dt):
					series,tags = known[dt]
					async with cl:
						r = await q.post(url, data=json.dumps(dict(aggregate={series:"last"},
							where=dict(s.split("=") for s in tags.split(" ")))))
						if r.raw:
							br = Resp(BufferedReader(data=r.raw))
							tag = await br.receive()
							time = parse_timestamp(await br.receive())
							val = await br.receive()
							ts = time.timestamp()
							t = time.strftime("%Y-%m-%d %H:%M:%S")
						else:
							ts=None
							t=None
						
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
									if sp and ts:
										if sp[0] > 0:
											if sp[1] is not None:
												lts = sp[0]+sp[1]
												if lts < ts:
													await s.send([dt, r['ts']*1000000000, 0], join=True)
											sp[1] = r['ts']
										else:
											ov = val
											if sp[1] is not None and val >= sp[1]:
												val -= sp[1]
											sp[1] = ov

									await s.send([dt,r['ts']*1000000000,val], join=True)
									n += 1
									t = r['t']
									ts = r['ts']
									cfg['at'] = {'t':t,'ts':ts,'dt':dt}
							await s.flush()
							if not n:
								break
						print(dt,*known[dt],nn)

				for k in known.keys():
					await one(k)

		N.cancel_scope.cancel()


trio.run(main)
