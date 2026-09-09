# Project status — plain language

**As of 2026-09-08 · Phase 0 complete · Phase 1 (the diagnostic tool) built · Phase 2 not started**

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

What is *not* built is the recovery workflow — the part that freezes
publication, recalculates, and pauses for human sign-off. That is Phase 2.

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

**5. Supporting work.** The pipeline runs as four automated workflows in
Airflow (the industry-standard scheduling tool) with 25 automated structural
tests passing; and a small, separate demonstration reproduces Fault A inside the
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

**The recovery workflow.** Freezing further publication, working out everything
affected, recalculating in the right order, then **pausing for human sign-off**
before telling outside parties that a published figure was wrong. That pause is
a deliberate design choice: no machine should notify a regulator or a client
unprompted. None of it exists yet — it is the whole of Phase 2.

**A few smaller pieces of the diagnostic tool are also deliberately unfinished,**
and it is worth being precise about which, because none of them affects whether
it finds the faults:

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
- **Reading the historical version automatically.** See the next section: the
  retrieval half works and is tested; the recording half is one line, not yet
  wired.

---

## Confidence and risk

**High confidence — verified by running it, on 2026-09-08:**

- Everything described under "What has been built" works as documented. Every
  headline figure quoted here was reproduced exactly.
- The results are reproducible from a clean start with five commands, and the
  faults can be switched on and off at will.

**Resolved since the last status:**

1. ~~The hardest part is still ahead.~~ **The biggest risk was retired first.**
   Reading the version of the instructions that ran on a given day was the
   least-proven part of the design, so it was investigated before anything was
   built. It works, and there is a test proving the tool reads genuinely
   historical instructions rather than today's. One caveat: the scheduler can
   record this automatically, but only if the pipeline is served from version
   control, which would slow daily development. The chosen compromise has the
   pipeline record its own version instead — the reading half is built and
   tested, the recording half is one line and not yet wired. Until it is, every
   trace is explicitly labelled as reading today's instructions, so no report
   can overstate what it knows.
2. ~~One measurement is not yet self-checking.~~ **It is now an automated
   test** — and writing it immediately found an error in the figure it was meant
   to protect. There is a 70th decision point on the path that the original
   count never looked for. The published breakdown was right about what it
   counted and one short of what there was to count. Both numbers are now
   locked down by tests.
3. ~~An open design question.~~ **Settled**, and implemented: the tool reports
   10 handoffs by default and can expand any one of them.

**Current risks:**

1. **Phase 2 is the larger half of the remaining work,** and it contains the
   only genuinely irreversible action in the project — telling an outside party
   that a published number was wrong. The human sign-off gate exists precisely
   to keep that decision with a person.
2. **Getting one check right took three attempts.** The single test that detects
   both faults — "did a value go missing here that was present a step earlier" —
   failed in two plausible ways before it was correct, each time confidently
   blaming an innocent stage. It is right now, with tests pinning both faults
   and the clean case. But it is a reminder that a diagnostic tool's failure mode
   is a *convincing wrong answer*, which is worse than no answer, and that
   deserves continued suspicion.
3. **Four documentation errors were found and corrected** across this and the
   previous review: a stale total that predated a calibration change, a
   miscounted number of business days, a missing warning that the verification
   harness leaves the test data in its broken state, and the missing 70th
   decision point. All four were documentation only — no processing logic was
   affected, and no reported figure changed.

**Nothing found in this review contradicts the project's central claim.** The
gap in existing tools is real, documented by the incumbent, and demonstrated
here with two faults that defeat their own controls and produce exact,
reproducible errors.

---

## Where things stand

| | |
|---|---|
| Phase 0 — test pipeline, planted faults, verification | **Complete and verified** |
| Phase 1 — the diagnostic engine | **Built and verified** (96 automated tests) |
| Phase 2 — the recovery workflow with human sign-off | **Not started** |

**Recommended next steps, in order:** decide whether to build the
stage-in-isolation capability, since two of the unfinished pieces above both
depend on it; wire the one-line version recording; then start Phase 2, leaving
enough time for the demonstration recording.
