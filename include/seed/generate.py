#!/usr/bin/env python3
"""Synthetic warehouse generator for de-branch-lineage.

Produces a DuckDB file holding the *raw* landing tables that the four SQL
modules under ``include/sql/`` transform. Volume is deliberately credible --
~50k trades over 22 business dates, 12 books, 8 counterparties, 5 currencies
-- because a pipeline you can trace by hand does not demonstrate anything.

Two bugs can be planted independently, so the pipeline can be shown healthy
and then broken without editing a line of SQL:

BUG A -- a JOIN failure.
    The GBP rate for 2026-09-03 is omitted from ``raw.fx_rates``. The LEFT
    JOIN in ``02_trades/05_convert_currency.sql`` misses, ``base_amount``
    goes NULL, the CASE in ``06_classify_product.sql`` falls to its ELSE
    arm, and SUM() silently skips the NULLs in
    ``03_positions/03_mark_to_market.sql``. It surfaces four modules later
    as a shortfall in ``04_reporting/03_desk_summary.sql``.

BUG B -- a FILTER failure.
    ``ref.calendar.accrual_period_end`` is seeded as the *last day* of the
    month instead of the *first day of the next month*. The accrual window
    predicate in ``03_positions/04_accrue_interest.sql`` uses a half-open
    interval (``>= start AND < end``), which is correct SQL against a correct
    calendar, so the predicate silently drops every trade booked on the last
    calendar day of a month.

Note that in both cases the SQL is byte-identical between the healthy and the
broken run. That is the point: the parser must see production-shaped SQL, not
SQL riddled with feature flags, and "the code didn't change, the reference
data did" is how these failures actually arrive in a bank.

Usage::

    python include/seed/generate.py                    # healthy
    python include/seed/generate.py --bug-a            # join failure
    python include/seed/generate.py --bug-b            # filter failure
    python include/seed/generate.py --bug-a --bug-b    # both
"""

from __future__ import annotations

import argparse
import datetime as dt
import math
import os
import random
from pathlib import Path

import duckdb
import pandas as pd

# --------------------------------------------------------------------------
# Shape of the synthetic book
# --------------------------------------------------------------------------

WINDOW_START = dt.date(2026, 8, 10)
WINDOW_DAYS = 30                       # -> 2026-08-10 .. 2026-09-08
N_TRADES = 50_000
DUPLICATE_RATE = 0.03                  # same trade landed twice by the feed
AMENDMENT_RATE = 0.05                  # a v2 row supersedes v1
CANCEL_RATE = 0.01

BUG_A_DATE = dt.date(2026, 9, 3)
BUG_A_CCY = "GBP"

CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CHF"]
CCY_WEIGHTS = [0.40, 0.20, 0.18, 0.12, 0.10]
CCY_BASE_RATE = {"USD": 1.0, "EUR": 1.0850, "GBP": 1.2740, "JPY": 0.00665, "CHF": 1.1240}

PRODUCTS = ["IRS", "FXFWD", "BOND", "EQTY", "REPO"]
PRODUCT_WEIGHTS = [0.26, 0.22, 0.22, 0.18, 0.12]

SOURCE_SYSTEMS = ["MUREX", "CALYPSO", "BOOKIT"]

# Desks are what 04_reporting/03_desk_summary.sql rolls up to.
BOOKS = [
    # book_code, book_name,            desk_code, desk_name,          division,   entity
    ("BK01", "Rates Flow London",      "DSK-RAT", "Rates",            "MARKETS",  "LE-UK"),
    ("BK02", "Rates Structured",       "DSK-RAT", "Rates",            "MARKETS",  "LE-UK"),
    ("BK03", "Rates Prop",             "DSK-RAT", "Rates",            "MARKETS",  "LE-US"),
    ("BK04", "FX Spot & Forward",      "DSK-FX",  "Foreign Exchange", "MARKETS",  "LE-UK"),
    ("BK05", "FX Options",             "DSK-FX",  "Foreign Exchange", "MARKETS",  "LE-CH"),
    ("BK06", "FX EM",                  "DSK-FX",  "Foreign Exchange", "MARKETS",  "LE-UK"),
    ("BK07", "Credit Flow",            "DSK-CRD", "Credit",           "MARKETS",  "LE-US"),
    ("BK08", "Credit Structured",      "DSK-CRD", "Credit",           "MARKETS",  "LE-UK"),
    ("BK09", "Equity Cash",            "DSK-EQD", "Equity Derivs",    "MARKETS",  "LE-US"),
    ("BK10", "Equity Derivatives",     "DSK-EQD", "Equity Derivs",    "MARKETS",  "LE-CH"),
    ("BK11", "Treasury Funding",       "DSK-TRE", "Treasury",         "BANKING",  "LE-UK"),
    ("BK12", "Treasury Liquidity",     "DSK-TRE", "Treasury",         "BANKING",  "LE-US"),
]

COUNTERPARTIES = [
    # cp_code, cp_name,                lei                   country rating netting
    ("CP01", "Northgate Asset Mgmt",  "213800AAAAAAAAAA0001", "GB", "A+",  True),
    ("CP02", "Helvetia Pension Fund", "213800AAAAAAAAAA0002", "CH", "AA-", True),
    ("CP03", "Meridian Global Bank",  "213800AAAAAAAAAA0003", "US", "AA",  True),
    ("CP04", "Kaskade Securities",    "213800AAAAAAAAAA0004", "DE", "BBB", False),
    ("CP05", "Orient Trust Corp",     "213800AAAAAAAAAA0005", "JP", "A",   True),
    ("CP06", "Bluefin Capital LLP",   "213800AAAAAAAAAA0006", "GB", "BBB+", False),
    ("CP07", "Aurora Reinsurance",    "213800AAAAAAAAAA0007", "US", "A-",  True),
    ("CP08", "Ridgeline Partners",    "213800AAAAAAAAAA0008", "GB", "BB+", False),
]

# Overnight funding rates by currency, used by 03_positions/04_accrue_interest.
CCY_OVERNIGHT_RATE = {"USD": 0.0432, "EUR": 0.0315, "GBP": 0.0468,
                      "JPY": 0.0038, "CHF": 0.0122}

# Deliberately empty. The real UK summer bank holiday falls on 2026-08-31, which
# is also the month end that BUG B targets. A holiday there would mean no trades
# were booked on that date at all, and BUG B would have nothing to drop. The
# holiday machinery in ref.calendar is exercised by the day_type CASE regardless.
UK_HOLIDAYS: set[dt.date] = set()

# Calibration. Mark drift is what turns into P&L; it is scaled so the GBP
# 2026-09-03 slice lands in the neighbourhood of the headline demo figure.
# TICKET_SIZE_SCALE is the single calibration knob for the demo. Every measure
# in the pipeline is linear in trade quantity, so the headline BUG A shortfall
# in rpt.desk_summary moves linearly with it. It is applied as a multiplier
# rather than by narrowing the randrange, because narrowing the range changes
# how many bits the RNG consumes and therefore reshuffles the entire book --
# which would make the healthy and broken runs incomparable.
#   1.0000 -> BUG A shortfall of ~237,000 USD
#   0.4468 -> BUG A shortfall of ~106,000 USD   (the figure the demo quotes)
TICKET_SIZE_SCALE = 0.4468

MARK_DRIFT_BPS = 4.4
DRIFT_NOISE_BPS = 2.2
BUY_RATIO = 0.62


# --------------------------------------------------------------------------
# Reference builders
# --------------------------------------------------------------------------

def _dates() -> list[dt.date]:
    return [WINDOW_START + dt.timedelta(days=i) for i in range(WINDOW_DAYS)]


def _business_dates() -> list[dt.date]:
    """Dates trades can actually be booked on -- weekdays that are not holidays."""
    return [d for d in _dates() if d.weekday() < 5 and d not in UK_HOLIDAYS]


def _last_day_of_month(d: dt.date) -> dt.date:
    if d.month == 12:
        return dt.date(d.year, 12, 31)
    return dt.date(d.year, d.month + 1, 1) - dt.timedelta(days=1)


def _first_day_of_next_month(d: dt.date) -> dt.date:
    if d.month == 12:
        return dt.date(d.year + 1, 1, 1)
    return dt.date(d.year, d.month + 1, 1)


def build_calendar(bug_b: bool) -> pd.DataFrame:
    """Trading calendar, one row per calendar date.

    ``accrual_period_end`` is the toggle for BUG B. Consumed as an *exclusive*
    upper bound by 03_positions/04_accrue_interest.sql, so it must be the
    first day of the following month. Seeded as the last day of the month it
    silently excludes every month-end booking.
    """
    rows = []
    for d in _dates():
        is_weekend = d.weekday() >= 5
        is_holiday = d in UK_HOLIDAYS
        rows.append(
            {
                "calendar_date": d,
                "is_business_day": not (is_weekend or is_holiday),
                "is_weekend": is_weekend,
                "holiday_name": "UK Summer Bank Holiday" if is_holiday else None,
                "accrual_period_start": d.replace(day=1),
                "accrual_period_end": (
                    _last_day_of_month(d) if bug_b else _first_day_of_next_month(d)
                ),
                "loaded_at": dt.datetime(2026, 9, 8, 2, 15, 0),
            }
        )
    return pd.DataFrame(rows)


def build_fx_rates(rng: random.Random, bug_a: bool) -> pd.DataFrame:
    """Daily FX rates against USD, with a duplicate-vendor feed to dedupe.

    BUG A removes the GBP row for 2026-09-03 entirely -- as if the vendor file
    landed short and nobody checked. Note the row is missing, not null: a null
    would have been caught by the not-null check in 02_load_fx_rates.sql.
    """
    rows = []
    for d in _dates():
        for ccy in CURRENCIES:
            if ccy == "USD":
                rate = 1.0
            else:
                base = CCY_BASE_RATE[ccy]
                drift = math.sin((d - WINDOW_START).days / 6.0) * 0.004
                rate = base * (1 + drift + rng.gauss(0, 0.0015))

            rows.append({
                "rate_date": d, "ccy": ccy, "quote_ccy": "USD",
                "rate": round(rate, 6), "rate_source": "REFINITIV",
                "ingested_at": dt.datetime.combine(d, dt.time(6, 30)),
            })
            # ~15% of dates get a late-arriving second vendor tick that the
            # dedupe in 02_load_fx_rates.sql has to resolve.
            if rng.random() < 0.15:
                rows.append({
                    "rate_date": d, "ccy": ccy, "quote_ccy": "USD",
                    "rate": round(rate * (1 + rng.gauss(0, 0.0002)), 6),
                    "rate_source": "BLOOMBERG",
                    "ingested_at": dt.datetime.combine(d, dt.time(18, 5)),
                })

    # BUG A is applied as a post-filter, deliberately. Skipping the row inside
    # the loop would also skip the rng.random() call below it, desynchronising
    # the generator's random stream and giving the healthy and broken runs
    # different trades. The delta between the two would then be meaningless.
    if bug_a:
        rows = [r for r in rows
                if not (r["ccy"] == BUG_A_CCY and r["rate_date"] == BUG_A_DATE)]

    return pd.DataFrame(rows)


def build_book_hierarchy() -> pd.DataFrame:
    rows = []
    for code, name, desk, desk_name, division, entity in BOOKS:
        rows.append({
            "book_code": code, "book_name": name,
            "desk_code": desk, "desk_name": desk_name,
            "division": division, "legal_entity": entity,
            "valid_from": dt.date(2024, 1, 1), "valid_to": dt.date(9999, 12, 31),
            "loaded_at": dt.datetime(2026, 9, 8, 2, 15, 0),
        })
    # One book was re-parented mid-window: an SCD-2 pair that
    # 03_load_book_hierarchy.sql has to collapse to the current record.
    rows.append({
        "book_code": "BK06", "book_name": "FX EM",
        "desk_code": "DSK-FX", "desk_name": "Foreign Exchange",
        "division": "MARKETS", "legal_entity": "LE-US",
        "valid_from": dt.date(2024, 1, 1), "valid_to": dt.date(2026, 6, 30),
        "loaded_at": dt.datetime(2026, 9, 8, 2, 15, 0),
    })
    return pd.DataFrame(rows)


def build_counterparty() -> pd.DataFrame:
    rows = []
    for code, name, lei, country, rating, netting in COUNTERPARTIES:
        rows.append({
            "cp_code": code, "cp_name": name, "cp_lei": lei,
            "country": country, "credit_rating": rating,
            "netting_agreement": netting,
            "valid_from": dt.date(2023, 1, 1), "valid_to": dt.date(9999, 12, 31),
            "loaded_at": dt.datetime(2026, 9, 8, 2, 15, 0),
        })
    return pd.DataFrame(rows)


def build_instruments(rng: random.Random) -> pd.DataFrame:
    rows = []
    for i in range(60):
        product = rng.choices(PRODUCTS, weights=PRODUCT_WEIGHTS)[0]
        ccy = rng.choices(CURRENCIES, weights=CCY_WEIGHTS)[0]
        rows.append({
            "instrument_id": f"INS{i:04d}",
            "instrument_name": f"{product} {ccy} {i:04d}",
            "product_type": product,
            "instrument_ccy": ccy,
            "reference_price": round(rng.uniform(40, 220), 4),
        })
    return pd.DataFrame(rows)


def build_marks(rng: random.Random, instruments: pd.DataFrame) -> pd.DataFrame:
    """End-of-day marks, one per instrument per date."""
    rows = []
    for _, ins in instruments.iterrows():
        px = float(ins["reference_price"])
        for d in _dates():
            px *= 1 + rng.gauss(MARK_DRIFT_BPS / 10_000, DRIFT_NOISE_BPS / 10_000)
            rows.append({
                "mark_date": d,
                "instrument_id": ins["instrument_id"],
                "mark_price": round(px, 6),
                "mark_source": "EOD-OFFICIAL",
                "ingested_at": dt.datetime.combine(d, dt.time(21, 0)),
            })
    return pd.DataFrame(rows)


def build_rate_curve() -> pd.DataFrame:
    rows = []
    for d in _dates():
        for ccy, r in CCY_OVERNIGHT_RATE.items():
            rows.append({
                "curve_date": d, "ccy": ccy, "tenor": "ON",
                "rate": r, "day_count_basis": 360,
            })
    return pd.DataFrame(rows)


def build_corporate_actions(rng: random.Random, instruments: pd.DataFrame) -> pd.DataFrame:
    """A handful of splits and dividends on the equity instruments."""
    eq = instruments[instruments["product_type"] == "EQTY"]
    rows = []
    for _, ins in eq.iterrows():
        if rng.random() < 0.30:
            d = WINDOW_START + dt.timedelta(days=rng.randrange(WINDOW_DAYS))
            action = rng.choice(["SPLIT", "DIVIDEND"])
            rows.append({
                "action_id": f"CA{len(rows):04d}",
                "instrument_id": ins["instrument_id"],
                "ex_date": d,
                "action_type": action,
                "ratio": 2.0 if action == "SPLIT" else 1.0,
                "cash_amount": 0.0 if action == "SPLIT" else round(rng.uniform(0.2, 1.8), 4),
            })
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------
# Trades
# --------------------------------------------------------------------------

def build_trades(rng: random.Random, instruments: pd.DataFrame) -> pd.DataFrame:
    """Raw trade feed: duplicates, amendments and cancellations included.

    Emitted deliberately dirty. 02_trades/02_dedupe.sql resolves the duplicate
    landings, 03_apply_amendments.sql applies the v2 supersession and the
    cancellations.
    """
    dates = _business_dates()
    ins_by_ccy: dict[str, list] = {c: [] for c in CURRENCIES}
    for _, r in instruments.iterrows():
        ins_by_ccy[r["instrument_ccy"]].append(r)

    rows = []
    for n in range(N_TRADES):
        trade_date = rng.choice(dates)
        ccy = rng.choices(CURRENCIES, weights=CCY_WEIGHTS)[0]
        ins = rng.choice(ins_by_ccy[ccy])
        book = rng.choice(BOOKS)[0]
        cp = rng.choice(COUNTERPARTIES)[0]
        direction = "BUY" if rng.random() < BUY_RATIO else "SELL"
        quantity = round(rng.randrange(50, 4_000) * TICKET_SIZE_SCALE, 2)
        price = round(float(ins["reference_price"]) * (1 + rng.gauss(0, 0.002)), 6)
        trade_id = f"TRD{n:07d}"
        base = {
            "trade_id": trade_id,
            "trade_version": 1,
            "event_ts": dt.datetime.combine(trade_date, dt.time(rng.randrange(7, 17),
                                                                rng.randrange(0, 60))),
            "trade_date": trade_date,
            "value_date": trade_date + dt.timedelta(days=2),
            "book_code": book,
            "cp_code": cp,
            "instrument_id": ins["instrument_id"],
            "product_type": ins["product_type"],
            "currency": ccy,
            "quantity": quantity,
            "price": price,
            "direction": direction,
            "trader_id": f"TR{rng.randrange(1, 41):03d}",
            "source_system": rng.choice(SOURCE_SYSTEMS),
            "amend_flag": False,
            "cancel_flag": False,
            "ingested_at": dt.datetime.combine(trade_date, dt.time(22, 0)),
        }
        rows.append(base)

        if rng.random() < DUPLICATE_RATE:
            dup = dict(base)
            dup["ingested_at"] = base["ingested_at"] + dt.timedelta(minutes=7)
            dup["source_system"] = "BOOKIT"
            rows.append(dup)

        if rng.random() < AMENDMENT_RATE:
            amd = dict(base)
            amd["trade_version"] = 2
            amd["quantity"] = round(quantity * rng.uniform(0.7, 1.3), 2)
            amd["amend_flag"] = True
            amd["event_ts"] = base["event_ts"] + dt.timedelta(hours=2)
            amd["ingested_at"] = base["ingested_at"] + dt.timedelta(hours=1)
            rows.append(amd)

        elif rng.random() < CANCEL_RATE:
            cxl = dict(base)
            cxl["trade_version"] = 2
            cxl["cancel_flag"] = True
            cxl["event_ts"] = base["event_ts"] + dt.timedelta(hours=3)
            cxl["ingested_at"] = base["ingested_at"] + dt.timedelta(hours=1)
            rows.append(cxl)

    return pd.DataFrame(rows)


# --------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--bug-a", action="store_true",
                    help="plant the JOIN failure (missing GBP FX rate for 2026-09-03)")
    ap.add_argument("--bug-b", action="store_true",
                    help="plant the FILTER failure (month-end accrual window off by one day)")
    ap.add_argument("--out", default=None,
                    help="output DuckDB path (default include/data/warehouse.duckdb)")
    ap.add_argument("--seed", type=int, default=20260903,
                    help="RNG seed; the same seed reproduces the same warehouse")
    args = ap.parse_args()

    out = Path(args.out) if args.out else (
        Path(__file__).resolve().parents[1] / "data" / "warehouse.duckdb")
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.exists():
        out.unlink()
    wal = out.with_suffix(out.suffix + ".wal")
    if wal.exists():
        wal.unlink()

    rng = random.Random(args.seed)

    print(f"seed={args.seed}  bug_a={args.bug_a}  bug_b={args.bug_b}")
    print(f"target: {out}")

    calendar = build_calendar(args.bug_b)
    fx = build_fx_rates(rng, args.bug_a)
    books = build_book_hierarchy()
    cps = build_counterparty()
    instruments = build_instruments(rng)
    marks = build_marks(rng, instruments)
    curve = build_rate_curve()
    actions = build_corporate_actions(rng, instruments)
    trades = build_trades(rng, instruments)

    flags = pd.DataFrame([
        {"flag_name": "bug_a_missing_fx_rate", "enabled": args.bug_a,
         "failure_class": "JOIN",
         "planted_in": "raw.fx_rates",
         "bites_in": "02_trades/05_convert_currency.sql",
         "surfaces_in": "04_reporting/03_desk_summary.sql",
         "detail": f"{BUG_A_CCY} rate for {BUG_A_DATE} omitted"},
        {"flag_name": "bug_b_month_end_accrual", "enabled": args.bug_b,
         "failure_class": "FILTER",
         "planted_in": "raw.calendar.accrual_period_end",
         "bites_in": "03_positions/04_accrue_interest.sql",
         "surfaces_in": "04_reporting/03_desk_summary.sql",
         "detail": "accrual_period_end seeded as month end, consumed as exclusive bound"},
    ])
    meta = pd.DataFrame([{
        "generated_at": dt.datetime.now(),
        "rng_seed": args.seed,
        "window_start": WINDOW_START,
        "window_end": WINDOW_START + dt.timedelta(days=WINDOW_DAYS - 1),
        "n_trades_requested": N_TRADES,
    }])

    con = duckdb.connect(str(out))
    for schema in ("raw", "ref", "trd", "pos", "rpt", "meta"):
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    tables = {
        "raw.calendar": calendar,
        "raw.fx_rates": fx,
        "raw.book_hierarchy": books,
        "raw.counterparty": cps,
        "raw.instruments": instruments,
        "raw.marks": marks,
        "raw.rate_curve": curve,
        "raw.corporate_actions": actions,
        "raw.trades": trades,
        "meta.pipeline_flags": flags,
        "meta.seed_run": meta,
    }
    for name, df in tables.items():
        con.register("_df", df)
        con.execute(f"CREATE OR REPLACE TABLE {name} AS SELECT * FROM _df")
        con.unregister("_df")
        print(f"  {name:28s} {len(df):>8,} rows")

    con.close()

    # The Airflow containers run as uid 50000 (astro) while this script normally
    # runs as the host user, so the bind-mounted warehouse would be read-only to
    # every task. Widen the mode on the file and its directory -- the directory
    # matters too, because DuckDB writes a sibling .wal alongside the database.
    # This is a local demo warehouse holding synthetic data; there is nothing
    # here to protect.
    try:
        os.chmod(out.parent, 0o777)
        os.chmod(out, 0o666)
    except OSError as exc:                       # pragma: no cover
        print(f"warning: could not relax permissions on {out}: {exc}")

    print(f"\nwrote {out}  ({out.stat().st_size / 1e6:.1f} MB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
