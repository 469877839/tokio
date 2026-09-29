# Priority-aware `Semaphore` — design notes

This change adds a priority to permit acquisition in the internal
`batch_semaphore.rs`, exposed through new methods on the public
`tokio::sync::Semaphore` in `semaphore.rs`. Existing `acquire` /
`acquire_many` (and their `_owned` / `blocking_` variants) are unchanged and
all use the default priority, so existing call sites compile and behave
exactly as before.

## Public API

* New `tokio::sync::Priority` enum: `Low` (the `Default`) and `High`.
* New entry points that take a priority:
  * `Semaphore::acquire_with_priority(Priority)`
  * `Semaphore::acquire_many_with_priority(n, Priority)`
  * `Semaphore::acquire_owned_with_priority(Priority)`
  * `Semaphore::acquire_many_owned_with_priority(n, Priority)`
  * the four corresponding `blocking_*` methods
* `try_acquire*` deliberately has no priority variant: it only inspects the
  atomic permit counter and never queues, so a priority would have no effect.

## Serving rule

Waiters are kept in the single arrival-ordered intrusive list that already
existed (oldest at the tail). On every permit release the next waiter is
chosen under the lock:

1. If the anti-starvation budget is exhausted, the **oldest** waiter is
   served next (with a partial grant if needed).
2. Otherwise the **oldest fully-satisfiable high-priority** waiter wins.
3. Otherwise the **oldest fully-satisfiable low-priority** waiter wins, but
   only if there is no older high-priority waiter still unserved.
4. Otherwise the permits **accumulate with the oldest waiter**, exactly as
   the old FIFO head accumulated them; it stays queued and completes on a
   later release.

The selection scans from the oldest waiter to the newest and only considers
the *first* waiter seen at each priority level.

### Why "the oldest waiter of a level blocks the whole level"

The original implementation only ever looked at the queue tail: when the
oldest waiter requested more permits than were available, every remaining
permit was assigned to it and no waiter behind it was touched. This is the
documented fairness guarantee of `acquire_many`, and it is load-bearing for
permit accounting — permits granted to a waiter are deducted from the
release batch, and a waiter that is granted permits but removed early would
either lose them or have to hand them back.

We therefore keep strict FIFO **within** a priority level and let priority
only reorder *across* levels:

* A newer `High` waiter may jump ahead of older `Low` waiters.
* A newer waiter never jumps ahead of an older waiter of the same priority.
* A `Low` waiter never runs while an older `High` waiter is still unserved.

This is the rule that is defensible "on both sides" of the batch-release
trade-off: when all waiters have one priority it is byte-for-byte the old
FIFO behaviour; when priorities differ it performs the priority reordering
that motivated the feature without ever splitting a level's queue.

### Batch releases

With the rule above a single batch behaves deterministically: fully
satisfiable high-priority waiters (oldest first) are served, then fully
satisfiable low-priority waiters, until the batch is exhausted or the oldest
remaining waiter cannot be completed, in which case the leftovers accumulate
on it. We do **not** "feed the low-priority head first and only then reach
past it to high-priority waiters", because that would let a large low
request delay interactive high requests — the problem being solved. Nor do
we hand leftovers to a newer waiter while an older waiter of the same level
is incomplete, because that is precisely the reordering the old FIFO
contract forbids.

## Anti-starvation

A pure priority queue lets continuous high traffic delay low requests
forever. Two candidate backstops were considered:

* **Time-based aging** ("a waiter that has waited N ms gets promoted"). This
  needs a clock/timer and an extra wakeup source; it also adds latency and
  platform-specific behaviour (and would pull timer dependencies into a
  primitive that currently needs none).
* **Overtake counting** ("after K high-priority overtakings, force the
  oldest waiter through"). This needs no timer, no allocation and no extra
  fields beyond one counter in the already-locked wait list.

We use overtake counting. `Waitlist::overtakes` is incremented each time a
newer high waiter is served while an older waiter is queued, and reset
whenever the oldest waiter is served (including a forced partial serve) or
the queue drains. After `MAX_OVERTAKES = 8` consecutive overtakings the
oldest waiter is granted the next permits even if it is low priority and
cannot be fully satisfied yet; it then accumulates the rest in FIFO order.

`8` is a fixed constant rather than a knob: it is large enough for one
release to batch a useful number of high-priority wakeups, and small enough
that a low waiter under sustained high load is guaranteed to make progress
after a tiny, bounded number of overtakings. The bound is on overtaking
*events*, not wall-clock time, so it holds without a timer and without
depending on how long each permit is held.

A forced serve that only partially grants the oldest waiter does not wake a
task spuriously: like the old FIFO head, it keeps the permits and completes
on a later release.

## Cancellation, closing, `forget`, and the fast path

* `Acquire` remains a cancel-safe future. If the future is dropped while
  queued, the node is unlinked and any permits already assigned to it are
  fed back through the same release routine, so permits are never lost and
  no waiter is left without a possible waker.
* `close` is untouched: it sets the closed bit under the lock and wakes
  every queued waiter, which returns the same `AcquireError` as before.
* The `forget_permits` / `SemaphorePermit::forget` path is untouched.
* The uncontended path is unchanged: `try_acquire` and the first CAS in
  `poll_acquire` are exactly as before, and `acquire` only additionally
  passes the immediate `Priority::Low` value. A queued waiter stores its
  priority in the existing `Waiter` node; the atomic permit counter and the
  fast path do not grow.
* No new dependencies are used, and `Semaphore::const_new` still constructs
  the semaphore in a `const` context (the only added `const`-constructible
  state is a `usize` counter initialised to zero).

## Verification

* New unit tests in `tokio/src/sync/tests/semaphore_batch.rs` cover
  high-over-low overtaking, same-level FIFO, batch ordering, the
  `MAX_OVERTAKES` force-through (including continuation within one batch),
  partial accumulation with level FIFO, cancellation after a partial grant,
  and close-while-priority-waiting.
* New public integration tests in `tokio/tests/sync_semaphore.rs` cover the
  borrowed and owned priority entries and the fact that the default
  `acquire` is low priority.
* A new loom model `priority_acquire_cancellation` joins the existing
  semaphore loom suite; the full `loom_semaphore_batch` and `loom_rwlock`
  suites pass under `--cfg loom`.
