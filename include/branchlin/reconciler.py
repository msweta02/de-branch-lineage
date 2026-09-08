"""Does each hop balance?

For every hop the tracer visits, the reconciler answers one question: does
the output of this hop account for its input? Concretely -- do the rows in
equal the rows out plus the rows each branch is *supposed* to drop, and does
the measure (a sum, a count, a net amount) survive the transformation.

A hop that balances is exonerated: whatever went wrong happened upstream.
The first hop that does *not* balance is the root cause, and everything
upstream of it is marked untrusted -- not because it is known to be wrong,
but because the trace reached it through logic that is itself broken, so its
evidence cannot be relied upon.

Not implemented. Phase 1.
"""
