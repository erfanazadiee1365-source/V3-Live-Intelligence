"""One-shot V3 live radar scan for scheduled GitHub Actions runs."""

import asyncio
import json
import math
import os
import time
from pathlib import Path
from statistics import mean, pstdev

import aiohttp
import aiosqlite


BASE = Path(__file__).resolve().parent
CONFIG_PATH = BASE / "config.json"


def load_config():
    if not CONFIG_PATH.is_file():
        raise FileNotFoundError(f"Missing config file: {CONFIG_PATH.name}")

    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    if not isinstance(config, dict):
        raise ValueError("config.json must contain a JSON object")

    config["rest_stats_url"] = (
        config.get("rest_stats_url")
        or config.get("market_stats_url")
        or "https://apiv2.nobitex.ir/market/stats"
    )
    config.setdefault("database", "v3_live_intelligence.sqlite3")
    config.setdefault("minimum_snapshots", 1)
    config.setdefault("no_chase_24h_pct", 25)
    config.setdefault("radar_candidate_score", 5)
    config.setdefault("rapid_wake_score", 8)
    config.setdefault("top_n", 20)
    config.setdefault("request_timeout", 20)

    if int(config["minimum_snapshots"]) < 1:
        config["minimum_snapshots"] = 1

    if (
        not isinstance(config["rest_stats_url"], str)
        or not config["rest_stats_url"].startswith("https://")
    ):
        raise ValueError("Nobitex stats URL must be an https URL")

    return config


CFG = load_config()
DB = Path(CFG["database"])
if not DB.is_absolute():
    DB = BASE / DB


def num(value):
    try:
        result = float(value)
        return result if math.isfinite(result) else 0.0
    except (TypeError, ValueError, OverflowError):
        return 0.0


def pct(new, old):
    return (new / old - 1.0) * 100.0 if old else 0.0


def zscore(value, history_values):
    if len(history_values) < 5:
        return 0.0
    deviation = pstdev(history_values)
    return 0.0 if deviation == 0 else (value - mean(history_values)) / deviation


def history_class(day_change):
    change = abs(day_change)
    if change < 3:
        return "H0-H1"
    if change < 8:
        return "H2"
    if change < 15:
        return "H3"
    if change < 25:
        return "H4"
    return "H5"


async def schema(db):
    await db.executescript(
        """
        PRAGMA journal_mode=WAL;

        CREATE TABLE IF NOT EXISTS snapshots(
          ts INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          price REAL,
          day_change REAL,
          volume_src REAL,
          turnover REAL,
          bid REAL,
          ask REAL,
          low REAL,
          high REAL,
          PRIMARY KEY(ts, symbol)
        );

        CREATE TABLE IF NOT EXISTS radar(
          ts INTEGER NOT NULL,
          symbol TEXT NOT NULL,
          score REAL,
          radar_a REAL,
          radar_b REAL,
          radar_c REAL,
          dprice REAL,
          dturn REAL,
          turn_z REAL,
          state_velocity REAL,
          history_state TEXT,
          status TEXT,
          PRIMARY KEY(ts, symbol)
        );

        CREATE TABLE IF NOT EXISTS discovery_ledger(
          first_seen_ts INTEGER NOT NULL,
          symbol TEXT PRIMARY KEY,
          first_score REAL,
          first_status TEXT,
          first_history_state TEXT,
          first_price REAL
        );

        CREATE TABLE IF NOT EXISTS health(
          ts INTEGER PRIMARY KEY,
          market_count INTEGER,
          fetch_ms INTEGER,
          gap_seconds INTEGER,
          ok INTEGER,
          message TEXT
        );
        """
    )
    await db.commit()


async def fetch_stats(session):
    started = time.monotonic()

    async with session.get(CFG["rest_stats_url"]) as response:
        response.raise_for_status()
        data = await response.json(content_type=None)

    elapsed_ms = int((time.monotonic() - started) * 1000)

    if not isinstance(data, dict) or data.get("status") != "ok":
        raise RuntimeError(
            "Unexpected Nobitex stats response: " + repr(data)[:250]
        )

    stats = data.get("stats")
    if not isinstance(stats, dict):
        raise RuntimeError("Nobitex response does not contain a stats object")

    return stats, elapsed_ms


def usdt_universe(stats):
    markets = {}

    for market, values in stats.items():
        if (
            not isinstance(values, dict)
            or not str(market).lower().endswith("-usdt")
        ):
            continue

        price = num(values.get("latest"))
        if price <= 0:
            continue

        symbol = str(market).replace("-", "").upper()
        markets[symbol] = {
            "price": price,
            "day_change": num(values.get("dayChange")),
            "volume_src": num(values.get("volumeSrc")),
            "turnover": num(values.get("volumeDst")),
            "bid": num(values.get("bestBuy")),
            "ask": num(values.get("bestSell")),
            "low": num(values.get("dayLow")),
            "high": num(values.get("dayHigh")),
        }

    return markets


async def save_snapshots(db, timestamp, markets):
    await db.executemany(
        "INSERT OR REPLACE INTO snapshots VALUES(?,?,?,?,?,?,?,?,?,?)",
        [
            (
                timestamp,
                symbol,
                *[
                    row[key]
                    for key in (
                        "price",
                        "day_change",
                        "volume_src",
                        "turnover",
                        "bid",
                        "ask",
                        "low",
                        "high",
                    )
                ],
            )
            for symbol, row in markets.items()
        ],
    )
    await db.commit()


async def history(db, symbol, limit=288):
    cursor = await db.execute(
        """
        SELECT ts, price, day_change, turnover, bid, ask, low, high
        FROM snapshots
        WHERE symbol=?
        ORDER BY ts DESC
        LIMIT ?
        """,
        (symbol, limit),
    )
    rows = await cursor.fetchall()
    return list(reversed(rows))


def evaluate(row, samples):
    # The first scan may have no earlier database snapshot.
    previous = samples[-2] if len(samples) >= 2 else None
    price_change = pct(row["price"], previous[1]) if previous else 0.0
    turnover_change = pct(row["turnover"], previous[3]) if previous else 0.0

    previous_turnover = [sample[3] for sample in samples[:-1]]
    turnover_z = zscore(row["turnover"], previous_turnover)

    velocity = 0.0
    if len(samples) >= 4:
        change_1 = pct(samples[-2][3], samples[-3][3])
        change_2 = pct(samples[-3][3], samples[-4][3])
        velocity = turnover_change - change_1 + (change_1 - change_2) * 0.5

    radar_a = (
        max(0.0, turnover_z) * 2.0
        + max(0.0, turnover_change) / 10.0
        - abs(price_change) / 3.0
    )
    radar_b = (
        max(0.0, price_change) * 1.6
        + max(0.0, velocity) / 12.0
        + max(0.0, row["day_change"]) / 5.0
    )

    if row["day_change"] > 15:
        radar_b -= (row["day_change"] - 15) * 1.5

    daily_range = row["high"] - row["low"]
    range_position = (
        (row["price"] - row["low"]) / daily_range
        if daily_range > 0
        else 0.5
    )

    radar_c = 0.0
    if 0.40 <= range_position <= 0.78 and 1 <= row["day_change"] <= 12:
        radar_c += 2.0
    if turnover_change > 0 and abs(price_change) < 2.0:
        radar_c += 1.0

    spread_pct = 0.0
    if row["bid"] > 0 and row["ask"] > 0:
        midpoint = (row["bid"] + row["ask"]) / 2.0
        if midpoint > 0:
            spread_pct = (row["ask"] - row["bid"]) / midpoint * 100.0

    score = radar_a + radar_b + radar_c - max(0.0, spread_pct - 0.5) * 2.0
    history_state = history_class(row["day_change"])

    if row["day_change"] >= num(CFG["no_chase_24h_pct"]):
        score -= 10.0

    if history_state == "H5":
        status = "NO_CHASE"
    elif score >= num(CFG["rapid_wake_score"]):
        status = "RAPID_WAKE"
    elif score >= num(CFG["radar_candidate_score"]):
        status = "RADAR"
    else:
        status = "OBSERVE"

    return {
        "score": score,
        "A": radar_a,
        "B": radar_b,
        "C": radar_c,
        "dprice": price_change,
        "dturn": turnover_change,
        "turn_z": turnover_z,
        "state_velocity": velocity,
        "H": history_state,
        "status": status,
        "baseline_ready": len(samples) >= 2,
    }


def atomic_json_write(path, payload):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    os.replace(temporary, path)


async def record_health(
    db, timestamp, market_count, fetch_ms, gap_seconds, ok, message
):
    await db.execute(
        "INSERT OR REPLACE INTO health VALUES(?,?,?,?,?,?)",
        (timestamp, market_count, fetch_ms, gap_seconds, int(ok), message[:500]),
    )
    await db.commit()


async def run():
    timestamp = int(time.time())
    previous_scan = None

    async with aiosqlite.connect(DB) as db:
        await schema(db)

        cursor = await db.execute("SELECT MAX(ts) FROM health WHERE ok=1")
        result = await cursor.fetchone()
        previous_scan = result[0] if result else None

        timeout_seconds = max(5, min(60, int(CFG.get("request_timeout", 20))))
        timeout = aiohttp.ClientTimeout(
            total=timeout_seconds,
            connect=min(10, timeout_seconds),
            sock_read=timeout_seconds,
        )
        headers = {"User-Agent": "V3-Live-Intelligence/1.0"}

        try:
            # Respect HTTP(S)_PROXY when the runner provides one.
            async with aiohttp.ClientSession(
                timeout=timeout,
                headers=headers,
                trust_env=True,
            ) as session:
                stats, fetch_ms = await fetch_stats(session)

            markets = usdt_universe(stats)
            if not markets:
                raise RuntimeError("No USDT markets were found in the stats response")

            await save_snapshots(db, timestamp, markets)

            ranked = []
            for symbol, row in markets.items():
                samples = await history(db, symbol)
                result = evaluate(row, samples)
                ranked.append((symbol, row, result))

                await db.execute(
                    "INSERT OR REPLACE INTO radar VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        timestamp,
                        symbol,
                        result["score"],
                        result["A"],
                        result["B"],
                        result["C"],
                        result["dprice"],
                        result["dturn"],
                        result["turn_z"],
                        result["state_velocity"],
                        result["H"],
                        result["status"],
                    ),
                )

                if result["status"] in ("RADAR", "RAPID_WAKE"):
                    await db.execute(
                        "INSERT OR IGNORE INTO discovery_ledger VALUES(?,?,?,?,?,?)",
                        (
                            timestamp,
                            symbol,
                            result["score"],
                            result["status"],
                            result["H"],
                            row["price"],
                        ),
                    )

            ranked.sort(key=lambda item: item[2]["score"], reverse=True)

            top = [
                {
                    "symbol": symbol,
                    "price": row["price"],
                    "day_change": row["day_change"],
                    **{
                        key: round(value, 4) if isinstance(value, float) else value
                        for key, value in result.items()
                    },
                }
                for symbol, row, result in ranked[
                    : max(1, int(CFG.get("top_n", 20)))
                ]
            ]

            gap = (
                0
                if previous_scan is None
                else max(0, timestamp - previous_scan)
            )
            await record_health(
                db, timestamp, len(markets), fetch_ms, gap, True, "ok"
            )

            atomic_json_write(
                BASE / "latest_radar.json",
                {
                    "timestamp": timestamp,
                    "markets": len(markets),
                    "health": "ok",
                    "top": top,
                },
            )

            print(
                f"V3 LIVE | markets={len(markets)} "
                f"| fetch={fetch_ms}ms | candidates={len(top)}"
            )

            for item in top[:12]:
                print(
                    f"{item['symbol']:12} {item['status']:11} "
                    f"score={item['score']:6.2f} "
                    f"A={item['A']:5.2f} B={item['B']:5.2f} "
                    f"C={item['C']:4.1f} "
                    f"24h={item['day_change']:6.2f}% {item['H']}"
                )

        except Exception as error:
            message = f"{type(error).__name__}: {error}"[:500]
            gap = (
                0
                if previous_scan is None
                else max(0, timestamp - previous_scan)
            )

            await record_health(db, timestamp, 0, 0, gap, False, message)

            atomic_json_write(
                BASE / "latest_radar.json",
                {
                    "timestamp": timestamp,
                    "markets": 0,
                    "health": "error",
                    "error": message,
                    "top": [],
                },
            )

            print("HEALTH ERROR:", message)
            raise


if __name__ == "__main__":
    asyncio.run(run())
