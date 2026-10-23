"""V3 Early-Wake challenger.

Reads the full persisted Nobitex USDT snapshot history and looks for
activity acceleration before price expansion. It does not place trades.
"""

import json
import math
import sqlite3
from pathlib import Path
from statistics import mean, pstdev


BASE = Path(__file__).resolve().parent
CONFIG_PATH = BASE / "config.json"
OUTPUT_PATH = BASE / "early_wake.json"


def num(value):
    try:
        result = float(value)
        return result if math.isfinite(result) else 0.0
    except (TypeError, ValueError, OverflowError):
        return 0.0


def pct(new, old):
    return (new / old - 1.0) * 100.0 if old else 0.0


def zscore(value, values):
    values = [num(v) for v in values if num(v) > 0]
    if len(values) < 5:
        return 0.0

    deviation = pstdev(values)
    if deviation == 0:
        return 0.0

    return (value - mean(values)) / deviation


def load_config():
    config = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    database = config.get("database", "v3_live_intelligence.sqlite3")
    return config, BASE / database


def symbols(conn):
    rows = conn.execute(
        "SELECT DISTINCT symbol FROM snapshots ORDER BY symbol"
    ).fetchall()
    return [row[0] for row in rows]


def history(conn, symbol, limit=36):
    return conn.execute(
        """
        SELECT ts, price, day_change, turnover, bid, ask, low, high
        FROM snapshots
        WHERE symbol=?
        ORDER BY ts DESC
        LIMIT ?
        """,
        (symbol, limit),
    ).fetchall()[::-1]


def evaluate(rows):
    if len(rows) < 2:
        return None

    current = rows[-1]
    previous = rows[-2]

    price = num(current[1])
    day_change = num(current[2])
    turnover = num(current[3])

    dprice = pct(price, num(previous[1]))
    dturn = pct(turnover, num(previous[3]))

    previous_dturn = 0.0
    older_dturn = 0.0

    if len(rows) >= 3:
        previous_dturn = pct(
            num(rows[-2][3]),
            num(rows[-3][3]),
        )

    if len(rows) >= 4:
        older_dturn = pct(
            num(rows[-3][3]),
            num(rows[-4][3]),
        )

    velocity = dturn - previous_dturn
    acceleration = velocity - (previous_dturn - older_dturn)

    turnover_history = [num(row[3]) for row in rows[:-1]]
    turn_z = zscore(turnover, turnover_history)

    activity_score = (
        max(0.0, dturn) / 6.0
        + max(0.0, velocity) / 10.0
        + max(0.0, turn_z) * 1.6
    )

    if abs(dprice) <= 1.25:
        price_lag_score = 2.0
    elif abs(dprice) <= 2.5:
        price_lag_score = 0.75
    else:
        price_lag_score = -2.0

    abs_day = abs(day_change)

    if abs_day <= 3:
        freshness_score = 2.0
    elif abs_day <= 5:
        freshness_score = 0.75
    elif abs_day <= 8:
        freshness_score = -1.0
    else:
        freshness_score = -3.0

    persistence_score = (
        1.0 if dturn > 0 and previous_dturn > 0 else 0.0
    )

    early_score = (
        activity_score
        + price_lag_score
        + freshness_score
        + persistence_score
    )

    late = abs_day >= 8 or abs(dprice) >= 5

    if late:
        state = "LATE_NO_CHASE"
    elif (
        early_score >= 10
        and dturn >= 8
        and velocity > 0
        and abs(dprice) <= 2.5
    ):
        state = "S4_EARLY_TRIGGER"
    elif (
        early_score >= 7
        and dturn >= 4
        and abs(dprice) <= 3
    ):
        state = "S3_PRESSURE"
    elif (
        early_score >= 5
        and dturn > 0
        and abs(dprice) <= 3
    ):
        state = "S2_EARLY_WAKE"
    else:
        state = "OBSERVE"

    return {
        "timestamp": int(current[0]),
        "price": price,
        "day_change": round(day_change, 4),
        "dprice": round(dprice, 4),
        "dturn": round(dturn, 4),
        "previous_dturn": round(previous_dturn, 4),
        "turn_z": round(turn_z, 4),
        "state_velocity": round(velocity, 4),
        "acceleration": round(acceleration, 4),
        "early_score": round(early_score, 4),
        "state": state,
        "baseline_samples": len(rows),
    }


def main():
    config, db_path = load_config()

    if not db_path.is_file():
        raise FileNotFoundError(f"Database not found: {db_path}")

    candidates = []
    late_no_chase = []
    observed = 0

    with sqlite3.connect(db_path) as conn:
        for symbol in symbols(conn):
            rows = history(conn, symbol)

            result = evaluate(rows)
            if result is None:
                continue

            observed += 1

            item = {
                "symbol": symbol,
                **result,
            }

            if result["state"] == "LATE_NO_CHASE":
                late_no_chase.append(item)
            elif result["state"] != "OBSERVE":
                candidates.append(item)

    candidates.sort(
        key=lambda item: item["early_score"],
        reverse=True,
    )

    late_no_chase.sort(
        key=lambda item: item["early_score"],
        reverse=True,
    )

    payload = {
        "engine": "V3_EARLY_WAKE_CHALLENGER",
        "mode": "OBSERVE_ONLY",
        "database": db_path.name,
        "markets_evaluated": observed,
        "candidate_count": len(candidates),
        "candidates": candidates,
        "late_no_chase": late_no_chase[:20],
        "rules": {
            "fresh_zone_24h_pct": 3,
            "late_zone_24h_pct": 8,
            "price_activity_lag_pct": 2.5,
            "s2_score": 5,
            "s3_score": 7,
            "s4_score": 10,
        },
    }

    temporary = OUTPUT_PATH.with_suffix(".json.tmp")
    temporary.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    temporary.replace(OUTPUT_PATH)

    print(
        "EARLY WAKE"
        f" | markets={observed}"
        f" | candidates={len(candidates)}"
    )

    for item in candidates[:15]:
        print(
            f"{item['symbol']:12}"
            f" {item['state']:17}"
            f" score={item['early_score']:7.2f}"
            f" dP={item['dprice']:7.2f}%"
            f" dT={item['dturn']:8.2f}%"
            f" z={item['turn_z']:6.2f}"
            f" v={item['state_velocity']:8.2f}"
            f" 24h={item['day_change']:7.2f}%"
        )


if __name__ == "__main__":
    main()
