# de-branch-lineage in plain language

This page explains the project for anyone who wants to understand it without
reading code. No technical background is assumed.

---

## The problem

When a number on a financial report is wrong, someone has to find out why.
Today that means working backwards by hand through a chain of automated steps,
often ten or more, asking at each one: "was the number still right here?" It
takes hours or days. It usually happens under pressure, because the wrong
number has often already been sent to someone who matters.

Tools exist that claim to help. They can tell you that a column called
`net_amount` was calculated from a column called `amt_net`. That's useful for
"where did this figure come from?", but it doesn't answer "why is it wrong?".
Being wrong is rarely about *which* columns fed a calculation. It's about a
decision the data took along the way:

- a rule that sorted a trade into the wrong category
- a filter that quietly left it out
- a lookup that found nothing

The market leader documents this gap itself: its tracing covers only the main
selection step and "doesn't reflect other usage like joins and filters". The
two faults planted in this project are, deliberately, a join and a filter.

---

## What was built

Think of it like a mechanic's diagnostic computer. Before you can trust one,
you need a car with a known fault to test it on, and you need to be sure that
fault is the only thing wrong.

**1. The car: a realistic pipeline.** It simulates a trading operation: about
54,000 trades across 22 business days, 12 trading books, 8 trading partners and
5 currencies. They flow through 20 processing steps in four stages: reference
data, trades, positions, then reports. It's deliberately too big to check by
hand, because a system you can check by eye proves nothing.

**2. The known faults: two of them, both hard to catch.** Each can be switched
on and off, and **the processing instructions are identical either way**. Only
the data changes. That's how these failures arrive in practice: nobody edited
the system, and some data quietly didn't show up.

- **Fault A: a missing exchange rate.** One day's GBP rate never arrives. It's
  *missing*, not blank. The pipeline has a check that rejects blanks, and a row
  that never arrived can't be rejected. There's also a coverage check meant to
  catch exactly this, and it passes, because it only looks at the first and last
  dates covered and the gap is in the middle. The report comes out **105,995.31
  USD short**, with no error, no blank cell and no missing row.
- **Fault B: a calendar off by one day.** A month-end date is recorded as the
  last day of the month instead of the first day of the next. The pipeline reads
  it in a way that would be correct against a correct calendar, so it silently
  leaves out trades booked on the last day of the month. The report comes out
  **22,017.08 USD short**, on one day a month, in a column nobody checks.

**3. The diagnostic computer: DIAGNOSE.** You give it only a wrong number on a
report. It walks back through the processing stages and stops at the one that
broke. It then names the specific decision responsible: for Fault A, the lookup
that found no exchange rate; for Fault B, the date filter. It does this without
being told where to look, and it stays silent when nothing is wrong. That
matters just as much, because a tool that cries wolf is a tool nobody runs.

**4. The repair: REMEDIATE.** Once a fault is found, a second workflow:

1. marks every affected report as not-for-publication
2. works out exactly what's downstream of the correction
3. recalculates those figures in the right order
4. **stops** and shows a person the wrong figure, the corrected figure, the
   difference and the cause

Only after that person approves is anything recorded as sent to an outside
party. In the demonstration, a report that was 105,995.31 USD short is restated
to the correct 607,617.49.

The notification is **deliberately fake**: it writes a row and sends nothing.
Telling a regulator or a client that a published number was wrong is the one
irreversible action in the project, and a demonstration shouldn't be one
misconfiguration away from doing it for real.

---

## Why the usual checks don't help

The pipeline has a reconciliation control, a check that exists to catch exactly
this kind of error. **It passes with both faults active.** It compares trade
*counts*, which match because no trade is ever lost. It also compares totals,
but against a figure that is itself downstream of the fault.

Counting rows doesn't help either. With Fault A active, every one of the 20
steps holds *exactly* the same number of rows as a clean run. By the time a
person sees the report, every conventional signal says the pipeline is healthy.

Between the missing rate and the wrong figure there are **11 stages, 10
handoffs and three departmental boundaries**. Inside them are **70 individual
decision points**, each of which a person would have to check by hand. That's
the size of the haystack, and the case for automating the search.

---

## Failing safely

This is the part most worth attention. One test deliberately breaks the
recalculation half-way through and checks what happens:

- the publication hold is released anyway
- nobody is asked to approve a repair that didn't finish
- nothing is sent outside
- the reports keep their original figures rather than a half-finished mixture
- the run is still reported as **failed**, so a successful cleanup can't
  disguise a failed repair

That combination is hard to get right in a simple script. The cleanup has to
run whether things went well or badly, it has to run even if the process is
killed, and running it must not make a broken repair look successful. It's the
main reason this lives inside a scheduler (Apache Airflow).

---

## How sure can you be?

- **167 automated tests pass**, including both the successful repair and the
  deliberately failed one.
- Every headline figure here can be reproduced exactly from a clean start, and
  the faults can be switched on and off at will. The steps are in
  [running-the-demo.md](running-the-demo.md).
- A screenshot record of a full live run is in
  [proof-of-concept.md](proof-of-concept.md).

**The most important lesson from building it:** the dangerous failure was never
a crash. It was a convincing wrong answer. Several times during development, a
step reported complete success while doing the wrong thing. One "corrected" a
number back to the original wrong value. Another restated nothing and reported
a difference of zero. Each was caught by a test rather than by review, and each
is now pinned down by one. A repair that fails loudly is safe; one that succeeds
while restating nothing isn't.

---

## What isn't finished

- **Knowing exactly which version of the instructions ran.** The design records
  this for every run, and a diagnosis is meant to read the instructions as they
  were on the day. Inside the scheduler that lookup doesn't work yet. The tool
  says so plainly ("UNPINNED") rather than presenting today's instructions as
  the ones that ran.
- **Explaining a near miss.** The tool can say "this row fell through to the
  last option" or "this lookup found nothing". It can't yet say "this row was
  one day inside the cut-off".
- **Narrowing the search more tightly.** When it can't carry a precise question
  backwards through a step, it asks a broader one. That's safe, because a
  broader question still contains the answer, but it does more work than it
  needs to.
- **A custom screen.** The scheduler's own approval screen is where the person
  decides. That was a deliberate trade: the time went into proving the failure
  path instead.
