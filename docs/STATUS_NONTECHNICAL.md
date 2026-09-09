# Project status — plain language

**As of 2026-09-09 · all three phases complete · 167 automated tests**

Audience: anyone who needs to know where this project stands without reading
code. No technical background assumed.

---

## The problem in one paragraph

When a number on a financial report is wrong, someone has to find out why. Today
that means a person working backwards by hand through a chain of automated steps
— often ten or more — asking at each one "was the number still right here?"
It takes hours or days, and it happens under deadline pressure, because the
wrong number has usually already been sent to someone who matters.

Tools exist that claim to help. They can tell you that a column called
`net_amount` was calculated from a column called `amt_net`. That is genuinely
useful for questions like "where did this figure come from". It does not answer
"why is it wrong", because being wrong is almost never about *which* columns fed
a calculation — it's about a decision the data took along the way. A rule that
sorted a trade into the wrong category. A filter that quietly excluded it. A
lookup that found nothing.

This project is aimed at that gap. The gap is not our claim about a competitor —
the market leader documents it themselves, stating their tracing covers only the
main selection statement and "doesn't reflect other usage like joins and
filters". The two faults we planted in this project are, deliberately, a join
and a filter.

---

## What has been built

Think of the goal as a diagnostic tool, the way a mechanic's diagnostic computer
is a tool. Before you can trust one, you need a car with a known fault to test
it against — and you need to be certain the fault is the only thing wrong.

Phase 0 built the car and the known faults. **Phase 1 built the diagnostic
computer, and it passes both tests.** Given only a wrong number on a report, it
walks back through ten processing stages and stops at the one that broke, naming
the specific decision responsible — for both planted faults, and without being
told where to look. It stays silent when nothing is wrong, which matters just as
much: a tool that cries wolf is a tool nobody runs.

**Phase 2 built the recovery workflow**, which is what happens after the fault
is found: hold the affected reports back from publication, recalculate them in
the right order once the data is corrected, then stop and ask a person before
telling anyone outside that a published figure was wrong.

Concretely, what exists and works today:

**1. A realistic pipeline.** A simulated trading operation: about 54,000 trades
across 22 business days, 12 trading books, 8 trading partners and 5 currencies,
flowing through 20 processing steps in four stages — reference data, trades,
positions, then reports. It is deliberately large enough that you cannot trace
it by hand, because a system you can check by eye doesn't prove anything.

**2. Two hidden faults, both of a kind that is genuinely hard to catch.** Each
can be switched on and off. The critical property: **the processing logic is
identical whether the fault is on or off.** Nothing in the instructions changes
— only the reference data does. That is how these failures actually arrive in
practice: nobody edited the system, some data quietly didn't show up.

- **Fault A — a missing exchange rate.** One day's GBP exchange rate never
  arrives. Note *missing*, not blank: the pipeline has a check that rejects
  blanks, and a row that never arrived cannot be rejected. There is also a
  coverage check whose job is to catch exactly this, and it passes — because it
  only verifies the first and last dates covered, and the gap is in the middle.
  The result is a report understated by **105,995.31 USD**, with no error
  message, no blank cell and no missing row anywhere on the report.

- **Fault B — an off-by-one-day calendar error.** A month-end date is recorded
  as the last day of the month rather than the first day of the next. The
  pipeline reads it in a way that is entirely correct against a correct
  calendar, and so silently excludes every trade booked on the last day of a
  month. The report comes back light by **22,017.08 USD**, one day a month, in a
  column nobody checks.

**3. Proof that the faults are measurable and correctly attributed.** An
automated harness rebuilds and re-runs the entire pipeline four times — clean,
fault A only, fault B only, both — and reports the difference. It confirms the
two faults don't interfere with each other, and that the underlying trade data
is *provably identical* in all four runs. That last point matters more than it
sounds: it means every difference in the results is caused by the fault and
nothing else. An early version failed this test — switching on a fault
accidentally changed which trades were generated, so the "difference" being
measured was partly a different set of trades. It was found and fixed.

**4. The most damning finding.** The pipeline contains a reconciliation control
— a check that exists specifically to catch this kind of error. **It passes
cleanly with both faults active.** It compares trade *counts*, which match
exactly because no trade is ever lost, and it compares totals against a figure
that is itself downstream of the fault.

And counting rows doesn't help either. With Fault A active, every one of the 20
processing steps holds *exactly* the same number of rows as a clean run. The
only thing that moves is a count of blank values — and that returns to zero
before it reaches the report. By the time a human sees the number, every
conventional signal says the pipeline is healthy.

**5. The recovery workflow, with a human in the loop.** Once a fault is found,
a second automated workflow: it marks every affected report as
not-for-publication, works out exactly what is downstream of the correction,
recalculates those figures in dependency order, and then **stops** — presenting
a person with the wrong figure, the corrected figure, the difference, the
decision that caused it, and the version of the instructions the original ran
under. Only after that person approves does anything get recorded as sent to an
outside party. Verified end to end: a report short by 105,995.31 USD is
restated to the correct 607,617.49.

The notification itself is **deliberately faked** — it writes a row to a table
and sends nothing. Telling a regulator or a client that a published number was
wrong is the only genuinely irreversible action in the project, and a
demonstration should not be one misconfiguration away from performing it for
real.

**6. Proof that it fails safely.** This is the part worth the most attention.
There is a test that deliberately breaks the recalculation halfway through, and
checks what the system does about it: the publication hold is released anyway,
nobody is asked to approve a recovery that did not finish, nothing is sent
outside, and the reports are left showing their original figures rather than a
half-finished mixture. The run is still reported as failed — a successful
cleanup is not allowed to disguise a failed recovery.

That combination is difficult to get right in a simple script and is the main
argument for building this inside a scheduler: the cleanup has to run whether
things went well or badly, it has to run even if the process is killed, and
getting it to run must not make a broken recovery look successful.

**7. Supporting work.** The pipeline runs as four automated workflows in
Airflow (the industry-standard scheduling tool) with structural tests on every
one of them; and a small, separate demonstration reproduces Fault A inside the
market-leading competitor's own tooling, showing its tracing does not point at
the cause.

---

## How far the fault has to travel

Between the missing exchange rate and the wrong figure on the report, there are
**11 stages, 10 handoffs and three departmental boundaries**. Inside those
stages are **69 individual decision points** — places where the data could go
one of several ways, each of which a human would currently have to check by
hand.

That number was measured by analysing the actual processing instructions, not
estimated. It is the size of the haystack, and the case for automating the
search.

---

## What has NOT been built

No graphical interface was built. The scheduler's own approval screen is where
the human decision happens, which was a deliberate trade: a custom interface
would have cost more time than it demonstrated, and the time went to proving the
failure path instead.

**A few smaller pieces of the diagnostic tool are deliberately unfinished,** and
it is worth being precise about which, because none of them affects whether it
finds the faults:

- **Explaining a near miss.** The tool can say "this row fell to the last
  option" or "this lookup found nothing". It cannot yet say "this row was one
  day inside the cut-off" — a phrasing that would be genuinely useful to a
  human. Doing it properly requires re-running individual stages in isolation,
  which is the next piece of engineering rather than a quick fix.
- **Narrowing the search more aggressively.** When the tool cannot carry a
  precise question backwards through a stage, it deliberately asks a *broader*
  question instead. That is safe — a broader question still contains the answer
  — but it means some traces do more work than strictly necessary. Three
  specific improvements are identified and not built.
There is nothing left on this list about reading the historical version of the
instructions — that is now working end to end (see below).

---

## Confidence and risk

**High confidence — verified by running it, on 2026-09-09:**

- Everything described under "What has been built" works as documented. Every
  headline figure quoted here was reproduced exactly.
- 167 automated tests pass with none skipped, including both the successful
  recovery and the deliberately-failed one.
- The results are reproducible from a clean start, and the faults can be
  switched on and off at will.

**Resolved since the last status:**

1. ~~The hardest part is still ahead.~~ **Retired, and now working end to
   end.** Reading the version of the instructions that ran on a given day was
   the least-proven part of the design, so it was investigated before anything
   else was built. The scheduler can record this automatically, but only if the
   pipeline is served from version control, which would have slowed daily
   development; so the pipeline records its own version instead. Both halves are
   now built, and a diagnosis names the exact version of the instructions that
   produced the day's figures. When it genuinely cannot establish one it says so
   in plain terms rather than quietly substituting today's, so no report can
   overstate what it knows.
2. ~~One measurement is not yet self-checking.~~ **It is now an automated
   test** — and writing it immediately found an error in the figure it was meant
   to protect. There is a 70th decision point on the path that the original
   count never looked for. The published breakdown was right about what it
   counted and one short of what there was to count. Both numbers are now
   locked down by tests.
3. ~~An open design question.~~ **Settled**, and implemented: the tool reports
   10 handoffs by default and can expand any one of them.

**Current risks:**

1. **The recurring failure mode of this project has been a convincing wrong
   answer, not a crash** — and it has now happened four times, each caught by a
   test rather than by review:
   - the check that detects both faults took three attempts; the first two
     confidently blamed an innocent stage;
   - the recalculation, when run as parallel independent steps, rebuilt reports
     from figures that had not been recalculated yet, reported complete success,
     and "corrected" the number to the original wrong value;
   - the recalculation twice started from the wrong place, missing precisely the
     steps that needed redoing, and again reported success with a difference of
     zero.

   All four are fixed and pinned by tests. The pattern is worth stating plainly:
   a recovery workflow that fails loudly is safe, and one that succeeds while
   restating nothing is not. Anything added to this later deserves the same
   suspicion.
2. **The irreversible action is mocked, and that boundary must be respected.**
   Approving the gate records a row; it does not contact anyone. Wiring a real
   channel is a small change and should not be made casually — the sign-off gate
   is what keeps that decision with a person, and it should stay in front of any
   real integration.
3. **Four documentation errors were found and corrected** across the reviews in
   this project: a stale total that predated a calibration change, a miscounted
   number of business days, a missing warning that the verification harness
   leaves the test data in its broken state, and the missing 70th decision
   point. All four were documentation only — no processing logic was affected,
   and no reported figure changed.

**Nothing found in this review contradicts the project's central claim.** The
gap in existing tools is real, documented by the incumbent, and demonstrated
here with two faults that defeat their own controls and produce exact,
reproducible errors.

---

## Where things stand

| | |
|---|---|
| Phase 0 — test pipeline, planted faults, verification | **Complete and verified** |
| Phase 1 — the diagnostic engine | **Complete and verified** |
| Phase 2 — the recovery workflow with human sign-off | **Complete and verified** |

All three phases are built, with 167 automated tests covering them. What remains
is presentation: the written summary and a three-minute recorded demonstration.

**Recommended next steps, in order:** record the demonstration, leading with the
deliberately-failed recovery rather than the successful one, since it is the
clearer argument; then, if time allows, build the ability to re-run a single
processing stage in isolation, because two of the unfinished pieces above both
depend on it.
