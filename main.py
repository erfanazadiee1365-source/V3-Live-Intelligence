import asyncio, json, math, time
from pathlib import Path
from statistics import mean, pstdev
import aiohttp, aiosqlite

BASE = Path(__file__).resolve().parent
CFG = json.loads((BASE/"config.json").read_text())
DB = BASE/CFG["database"]

def num(v):
    try: return float(v)
    except (TypeError, ValueError): return 0.0

def pct(new, old):
    return (new/old-1)*100 if old else 0.0

def zscore(x, xs):
    if len(xs) < 5: return 0.0
    s=pstdev(xs)
    return 0.0 if s == 0 else (x-mean(xs))/s

def history_class(day_change):
    a=abs(day_change)
    if a < 3: return "H0-H1"
    if a < 8: return "H2"
    if a < 15: return "H3"
    if a < 25: return "H4"
    return "H5"

async def schema(db):
    await db.executescript("""
    PRAGMA journal_mode=WAL;
    CREATE TABLE IF NOT EXISTS snapshots(
      ts INTEGER NOT NULL, symbol TEXT NOT NULL,
      price REAL, day_change REAL, volume_src REAL, turnover REAL,
      bid REAL, ask REAL, low REAL, high REAL,
      PRIMARY KEY(ts,symbol));
    CREATE TABLE IF NOT EXISTS radar(
      ts INTEGER NOT NULL, symbol TEXT NOT NULL,
      score REAL, radar_a REAL, radar_b REAL, radar_c REAL,
      dprice REAL, dturn REAL, turn_z REAL, state_velocity REAL,
      history_state TEXT, status TEXT,
      PRIMARY KEY(ts,symbol));
    CREATE TABLE IF NOT EXISTS discovery_ledger(
      first_seen_ts INTEGER NOT NULL, symbol TEXT PRIMARY KEY,
      first_score REAL, first_status TEXT, first_history_state TEXT,
      first_price REAL);
    CREATE TABLE IF NOT EXISTS health(
      ts INTEGER PRIMARY KEY, market_count INTEGER, fetch_ms INTEGER,
      gap_seconds INTEGER, ok INTEGER, message TEXT);
    """)
    await db.commit()

async def fetch_stats(session):
    t=time.time()
    async with session.get(CFG["rest_stats_url"],timeout=20) as r:
        r.raise_for_status()
        data=await r.json()
    if data.get("status")!="ok": raise RuntimeError(str(data)[:300])
    return data["stats"], int((time.time()-t)*1000)

def usdt_universe(stats):
    out={}
    for key,x in stats.items():
        if not isinstance(x,dict) or not key.lower().endswith("-usdt"): continue
        price=num(x.get("latest"))
        if price<=0: continue
        sym=key.replace("-","").upper()
        out[sym]={
          "price":price,"day_change":num(x.get("dayChange")),
          "volume_src":num(x.get("volumeSrc")),"turnover":num(x.get("volumeDst")),
          "bid":num(x.get("bestBuy")),"ask":num(x.get("bestSell")),
          "low":num(x.get("dayLow")),"high":num(x.get("dayHigh"))
        }
    return out

async def save_snapshots(db,ts,rows):
    await db.executemany("""INSERT OR REPLACE INTO snapshots VALUES(?,?,?,?,?,?,?,?,?,?)""",
      [(ts,s,*[r[k] for k in ("price","day_change","volume_src","turnover","bid","ask","low","high")])
       for s,r in rows.items()])
    await db.commit()

async def history(db,symbol,limit=288):
    cur=await db.execute("""SELECT ts,price,day_change,turnover,bid,ask,low,high
                            FROM snapshots WHERE symbol=? ORDER BY ts DESC LIMIT ?""",(symbol,limit))
    rows=await cur.fetchall()
    return list(reversed(rows))

def evaluate(r,h):
    # h includes current row at end. Use prior samples for baseline.
    prev=h[-2]
    dprice=pct(r["price"],prev[1])
    dturn=pct(r["turnover"],prev[3])
    prior_turn=[x[3] for x in h[:-1]]
    tz=zscore(r["turnover"],prior_turn)

    # Second difference proxy: recent turnover-change acceleration.
    state_velocity=0.0
    if len(h)>=4:
        d1=pct(h[-2][3],h[-3][3])
        d2=pct(h[-3][3],h[-4][3])
        state_velocity=dturn-d1 + (d1-d2)*0.5

    # A: activity wakes faster than price; strong for absorption/F1/F3.
    A=max(0,tz)*2.0 + max(0,dturn)/10.0 - abs(dprice)/3.0

    # B: momentum transition. Reward early movement, penalize mature 24h expansion.
    B=max(0,dprice)*1.6 + max(0,state_velocity)/12.0 + max(0,r["day_change"])/5.0
    if r["day_change"]>15: B -= (r["day_change"]-15)*1.5

    # C: controlled position within 24h range + positive but not mature expansion.
    rng=r["high"]-r["low"]
    pos=(r["price"]-r["low"])/rng if rng>0 else .5
    C=0.0
    if .40<=pos<=.78 and 1<=r["day_change"]<=12: C+=2.0
    if dturn>0 and abs(dprice)<2.0: C+=1.0

    spread=0.0
    if r["bid"]>0 and r["ask"]>0:
        mid=(r["bid"]+r["ask"])/2
        spread=(r["ask"]-r["bid"])/mid*100 if mid else 0

    score=A+B+C-max(0,spread-.5)*2
    H=history_class(r["day_change"])
    if r["day_change"]>=CFG["no_chase_24h_pct"]: score-=10

    if H=="H5": status="NO_CHASE"
    elif score>=CFG["rapid_wake_score"]: status="RAPID_WAKE"
    elif score>=CFG["radar_candidate_score"]: status="RADAR"
    else: status="OBSERVE"
    return dict(score=score,A=A,B=B,C=C,dprice=dprice,dturn=dturn,turn_z=tz,
                state_velocity=state_velocity,H=H,status=status)

async def run():
    last_ts=None
    async with aiohttp.ClientSession(headers={"User-Agent":"V3-Live-Intelligence/1.0"}) as session:
      async with aiosqlite.connect(DB) as db:
        await schema(db)
        while True:
          ts=int(time.time())
          try:
            stats,fetch_ms=await fetch_stats(session)
            rows=usdt_universe(stats)
            await save_snapshots(db,ts,rows)
            ranked=[]
            for sym,r in rows.items():
                h=await history(db,sym)
                if len(h)<CFG["minimum_snapshots"]: continue
                e=evaluate(r,h)
                ranked.append((sym,r,e))
                await db.execute("""INSERT OR REPLACE INTO radar VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                  (ts,sym,e["score"],e["A"],e["B"],e["C"],e["dprice"],e["dturn"],
                   e["turn_z"],e["state_velocity"],e["H"],e["status"]))
                if e["status"] in ("RADAR","RAPID_WAKE"):
                    await db.execute("""INSERT OR IGNORE INTO discovery_ledger VALUES(?,?,?,?,?,?)""",
                      (ts,sym,e["score"],e["status"],e["H"],r["price"]))
            gap=0 if last_ts is None else ts-last_ts
            await db.execute("INSERT OR REPLACE INTO health VALUES(?,?,?,?,?,?)",
                             (ts,len(rows),fetch_ms,gap,1,"ok"))
            await db.commit(); last_ts=ts
            ranked.sort(key=lambda x:x[2]["score"],reverse=True)
            export=[{"symbol":s,"price":r["price"],"day_change":r["day_change"],
                     **{k:round(v,4) if isinstance(v,float) else v for k,v in e.items()}}
                    for s,r,e in ranked[:25]]
            (BASE/"latest_radar.json").write_text(json.dumps(
                {"timestamp":ts,"markets":len(rows),"top":export},indent=2),encoding="utf-8")
            print(f"\nV3 LIVE | markets={len(rows)} | fetch={fetch_ms}ms")
            for x in export[:12]:
                print(f"{x['symbol']:12} {x['status']:11} score={x['score']:6.2f} "
                      f"A={x['A']:5.2f} B={x['B']:5.2f} C={x['C']:4.1f} "
                      f"24h={x['day_change']:6.2f}% {x['H']}")
          except Exception as ex:
            msg=repr(ex)[:250]
            gap=0 if last_ts is None else ts-last_ts
            await db.execute("INSERT OR REPLACE INTO health VALUES(?,?,?,?,?,?)",
                             (ts,0,0,gap,0,msg))
            await db.commit()
            print("HEALTH ERROR:",msg)
          await asyncio.sleep(CFG["poll_seconds"])

if __name__=="__main__":
    asyncio.run(run())
