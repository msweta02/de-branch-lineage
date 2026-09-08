# The two planted bugs

Both are toggled from `include/seed/generate.py`. The SQL is **byte-identical**
between the healthy and the broken run — only the seeded data changes. That is
deliberate on two counts: the parser must see production-shaped SQL rather than
SQL riddled with feature flags, and "the code didn't change, the reference data
did" is how these failures actually arrive.

    python include/seed/generate.py            # healthy
    python include/seed/generate.py --bug-a    # join failure
    python include/seed/generate.py --bug-b    # filter failure
    python include/seed/generate.py --bug-a --bug-b

All four scenarios produce a byte-identical `raw.trades` — 54,475 rows, gross
14,203,360,943.73 — so any delta between them is attributable to the bug and
nothing else. See BUILD_LOG.md, "Surprise 4", for why this took a fix.

---

## BUG A — a JOIN failure

**Planted in:** `raw.fx_rates` — the GBP rate for 2026-09-03 never lands.
**Bites in:** `02_trades/05_convert_currency.sql`
**Surfaces in:** `04_reporting/03_desk_summary.sql`, as **−105,995.31 USD**

The rate is *missing*, not null. A null would have been rejected by the
not-null filter in `01_reference/02_load_fx_rates.sql`. A row that never
arrived cannot be rejected by a filter on the table it is not in.

### The chain

| # | relation | what happens |
|---|---|---|
| 1 | `ref.fx_rates` | 150 rows become 149. Nothing complains. |
| 2 | `ref.validity_window` | The coverage control computes `min(rate_date)`/`max(rate_date)` per currency. A hole in the middle of the range sits inside those bounds and passes clean. |
| 3 | `trd.trades_converted` | `LEFT JOIN ref.fx_rates ON currency, trade_date` misses for 410 GBP trades. `base_amount` becomes NULL. The join is LEFT on purpose — an INNER join would delete the trade, and a vanished trade is worse than an unvalued one. So **no row is lost**. |
| 4 | `trd.trades_classified` | The size-tier `CASE` tests `base_amount >= 25000000`. `NULL >= 25000000` is NULL, not TRUE, so all four WHEN arms fail and the row lands in the `ELSE` arm — `TIER_5_RESIDUAL`, written as a catch-all for tiny tickets. |
| 5 | `pos.book_positions` | `SUM(base_amount)` over the group. 96 position rows go NULL. |
| 6-8 | `pos.positions_adjusted` → `positions_marked` → `positions_accrued` | The NULL propagates through corporate actions, mark-to-market and accrual. Still no error. |
| 9 | `pos.positions_netted` | 197 rows NULL. |
| 10 | `rpt.pnl_daily` | `COALESCE(mtm, 0) + COALESCE(accrued, 0)`. **The NULL is absorbed.** From here the shortfall is invisible — the report carries no null at all. |
| 11-12 | `rpt.regulatory_extract` → `rpt.desk_summary` | The number is simply smaller. |

Eleven relations on the causal path, ten hops, **three module boundaries**, and
**69 reconcilable branch points** — 50 CASE arms, 11 join conditions and 8
WHERE conjuncts — spread over 30 CTE-level intermediate steps.

These are counted with sqlglot over the eleven files on the path, not
estimated. `ref.validity_window` is excluded: it is a side input that never
carries `base_amount`.

| granularity | distance from cause to symptom |
|---|---:|
| relations | 11 |
| hops (edges) | 10 |
| CTE-level steps | 30 |
| branch points the reconciler will step through | 69 |

Note that the brief asked for 15+ hops. At *file* granularity the 20-file
layout cannot deliver that on one straight line — reference feeds trades at
file 5 of 6. At every granularity the reconciler actually works in, the
distance is comfortably past 15.

### Why the control does not catch it

`04_reporting/04_recon_control.sql` exists to catch exactly this and returns
`RECONCILED` in every scenario. Two reasons, both realistic:

1. It reconciles trade **counts**, which tie perfectly — BUG A loses no rows.
2. Its amount check compares `rpt.desk_summary` against `pos.positions_netted`,
   which is itself downstream of the break. Both sides are wrong by the same
   amount, so the difference is zero.

A control that reconciles a number against a number derived from it is not a
control.

---

## BUG B — a FILTER failure

**Planted in:** `ref.calendar.accrual_period_end`
**Bites in:** `03_positions/04_accrue_interest.sql`
**Surfaces in:** `04_reporting/03_desk_summary.sql`, as **−22,017.08 USD** on 2026-08-31

`accrual_period_end` is documented as an *exclusive* upper bound — the first day
of the following month. It is seeded as the **last day of the month** instead.

The accrual window predicate is a standard half-open interval:

```sql
WHERE p.calendar_date >= p.accrual_period_start
  AND p.calendar_date <  p.accrual_period_end
```

This is correct SQL against a correct calendar. Against this one it silently
excludes every date that *is* the month end — 2026-08-31 in the seeded window.
The `accrual_basis` CTE drops from 150 rows to 145, and 678 positions left-join
to nothing and accrue zero.

Positions are LEFT JOINed to the basis rather than filtered by it, so the rows
survive and the day does **not** vanish from the report. It just comes back
light, on one day a month, in a column nobody reconciles. An earlier version
used a hard inner filter; it deleted 2026-08-31 from `rpt.desk_summary`
entirely, which anybody would notice on the next morning's report.

---

## Why these two

dbt's column-level lineage documentation states that it reflects lineage from
`SELECT` statements and "doesn't reflect other usage like **joins and
filters**". One join, one filter — deliberately the two things the incumbent
documents that it does not cover.

## What row counts tell you: nothing

Under BUG A every relation holds exactly the same number of rows as the healthy
run, at all twenty hops. Only the null count moves — and by `rpt.pnl_daily` it
is back to zero.

| relation | healthy | bug_a | bug_b |
|---|---:|---:|---:|
| `trd.trades_converted` | 49,508 / 0 null | 49,508 / **410** | 49,508 / 0 |
| `trd.trades_classified` | 49,508 / 0 | 49,508 / **410** | 49,508 / 0 |
| `pos.book_positions` | 14,727 / 0 | 14,727 / **96** | 14,727 / 0 |
| `pos.positions_marked` | 14,727 / 0 | 14,727 / **96** | 14,727 / 0 |
| `pos.positions_accrued` | 14,727 / 0 | 14,727 / 96 | 14,727 / **678** |
| `pos.positions_netted` | 19,779 / 0 | 19,779 / **197** | 19,779 / 0 |
| `rpt.pnl_daily` | 6,138 / 0 | 6,138 / **0** | 6,138 / 0 |
| `rpt.desk_summary` | 110 / 0 | 110 / **0** | 110 / 0 |

Regenerate this table with `python tests/verify_scenarios.py`.
