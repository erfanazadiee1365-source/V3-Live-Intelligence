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

    # Accept the URL key used by the existing config and newer versions.
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
        if not isinstance(values, dict):
            continue

        if not str(market).lower().endswith("-usdt"):
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
    # GitHub may start with an empty database, so the first scan has no
