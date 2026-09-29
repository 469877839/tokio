use crate::sync::batch_semaphore::Semaphore;
use tokio_test::*;

const MAX_PERMITS: usize = crate::sync::Semaphore::MAX_PERMITS;

#[cfg(all(target_family = "wasm", target_os = "unknown"))]
use wasm_bindgen_test::wasm_bindgen_test as test;

#[test]
fn poll_acquire_one_available() {
    let s = Semaphore::new(100);
    assert_eq!(s.available_permits(), 100);

    // Polling for a permit succeeds immediately
    assert_ready_ok!(task::spawn(s.acquire(1)).poll());
    assert_eq!(s.available_permits(), 99);
}

#[test]
fn poll_acquire_many_available() {
    let s = Semaphore::new(100);
    assert_eq!(s.available_permits(), 100);

    // Polling for a permit succeeds immediately
    assert_ready_ok!(task::spawn(s.acquire(5)).poll());
    assert_eq!(s.available_permits(), 95);

    assert_ready_ok!(task::spawn(s.acquire(5)).poll());
    assert_eq!(s.available_permits(), 90);
}

#[test]
fn try_acquire_one_available() {
    let s = Semaphore::new(100);
    assert_eq!(s.available_permits(), 100);

    assert_ok!(s.try_acquire(1));
    assert_eq!(s.available_permits(), 99);

    assert_ok!(s.try_acquire(1));
    assert_eq!(s.available_permits(), 98);
}

#[test]
fn try_acquire_many_available() {
    let s = Semaphore::new(100);
    assert_eq!(s.available_permits(), 100);

    assert_ok!(s.try_acquire(5));
    assert_eq!(s.available_permits(), 95);

    assert_ok!(s.try_acquire(5));
    assert_eq!(s.available_permits(), 90);
}

#[test]
fn poll_acquire_one_unavailable() {
    let s = Semaphore::new(1);

    // Acquire the first permit
    assert_ready_ok!(task::spawn(s.acquire(1)).poll());
    assert_eq!(s.available_permits(), 0);

    let mut acquire_2 = task::spawn(s.acquire(1));
    // Try to acquire the second permit
    assert_pending!(acquire_2.poll());
    assert_eq!(s.available_permits(), 0);

    s.release(1);

    assert_eq!(s.available_permits(), 0);
    assert!(acquire_2.is_woken());
    assert_ready_ok!(acquire_2.poll());
    assert_eq!(s.available_permits(), 0);

    s.release(1);
    assert_eq!(s.available_permits(), 1);
}

#[test]
fn poll_acquire_many_unavailable() {
    let s = Semaphore::new(5);

    // Acquire the first permit
    assert_ready_ok!(task::spawn(s.acquire(1)).poll());
    assert_eq!(s.available_permits(), 4);

    // Try to acquire the second permit
    let mut acquire_2 = task::spawn(s.acquire(5));
    assert_pending!(acquire_2.poll());
    assert_eq!(s.available_permits(), 0);

    // Try to acquire the third permit
    let mut acquire_3 = task::spawn(s.acquire(3));
    assert_pending!(acquire_3.poll());
    assert_eq!(s.available_permits(), 0);

    s.release(1);

    assert_eq!(s.available_permits(), 0);
    assert!(acquire_2.is_woken());
    assert_ready_ok!(acquire_2.poll());

    assert!(!acquire_3.is_woken());
    assert_eq!(s.available_permits(), 0);

    s.release(1);
    assert!(!acquire_3.is_woken());
    assert_eq!(s.available_permits(), 0);

    s.release(2);
    assert!(acquire_3.is_woken());

    assert_ready_ok!(acquire_3.poll());
}

#[test]
fn try_acquire_one_unavailable() {
    let s = Semaphore::new(1);

    // Acquire the first permit
    assert_ok!(s.try_acquire(1));
    assert_eq!(s.available_permits(), 0);

    assert_err!(s.try_acquire(1));

    s.release(1);

    assert_eq!(s.available_permits(), 1);
    assert_ok!(s.try_acquire(1));

    s.release(1);
    assert_eq!(s.available_permits(), 1);
}

#[test]
fn try_acquire_many_unavailable() {
    let s = Semaphore::new(5);

    // Acquire the first permit
    assert_ok!(s.try_acquire(1));
    assert_eq!(s.available_permits(), 4);

    assert_err!(s.try_acquire(5));

    s.release(1);
    assert_eq!(s.available_permits(), 5);

    assert_ok!(s.try_acquire(5));

    s.release(1);
    assert_eq!(s.available_permits(), 1);

    s.release(1);
    assert_eq!(s.available_permits(), 2);
}

#[test]
fn poll_acquire_one_zero_permits() {
    let s = Semaphore::new(0);
    assert_eq!(s.available_permits(), 0);

    // Try to acquire the permit
    let mut acquire = task::spawn(s.acquire(1));
    assert_pending!(acquire.poll());

    s.release(1);

    assert!(acquire.is_woken());
    assert_ready_ok!(acquire.poll());
}

#[test]
fn max_permits_doesnt_panic() {
    Semaphore::new(MAX_PERMITS);
}

#[test]
#[should_panic]
#[cfg(not(target_family = "wasm"))] // wasm currently doesn't support unwinding
fn validates_max_permits() {
    Semaphore::new(MAX_PERMITS + 1);
}

#[test]
fn close_semaphore_prevents_acquire() {
    let s = Semaphore::new(5);
    s.close();

    assert_eq!(5, s.available_permits());

    assert_ready_err!(task::spawn(s.acquire(1)).poll());
    assert_eq!(5, s.available_permits());

    assert_ready_err!(task::spawn(s.acquire(1)).poll());
    assert_eq!(5, s.available_permits());
}

#[test]
fn close_semaphore_notifies_permit1() {
    let s = Semaphore::new(0);
    let mut acquire = task::spawn(s.acquire(1));

    assert_pending!(acquire.poll());

    s.close();

    assert!(acquire.is_woken());
    assert_ready_err!(acquire.poll());
}

#[test]
fn close_semaphore_notifies_permit2() {
    let s = Semaphore::new(2);

    // Acquire a couple of permits
    assert_ready_ok!(task::spawn(s.acquire(1)).poll());
    assert_ready_ok!(task::spawn(s.acquire(1)).poll());

    let mut acquire3 = task::spawn(s.acquire(1));
    let mut acquire4 = task::spawn(s.acquire(1));
    assert_pending!(acquire3.poll());
    assert_pending!(acquire4.poll());

    s.close();

    assert!(acquire3.is_woken());
    assert!(acquire4.is_woken());

    assert_ready_err!(acquire3.poll());
    assert_ready_err!(acquire4.poll());

    assert_eq!(0, s.available_permits());

    s.release(1);

    assert_eq!(1, s.available_permits());

    assert_ready_err!(task::spawn(s.acquire(1)).poll());

    s.release(1);

    assert_eq!(2, s.available_permits());
}

#[test]
fn cancel_acquire_releases_permits() {
    let s = Semaphore::new(10);
    s.try_acquire(4).expect("uncontended try_acquire succeeds");
    assert_eq!(6, s.available_permits());

    let mut acquire = task::spawn(s.acquire(8));
    assert_pending!(acquire.poll());

    assert_eq!(0, s.available_permits());
    drop(acquire);

    assert_eq!(6, s.available_permits());
    assert_ok!(s.try_acquire(6));
}

#[test]
fn release_permits_at_drop() {
    use crate::sync::semaphore::*;
    use futures::task::ArcWake;
    use std::future::Future;
    use std::sync::Arc;

    let sem = Arc::new(Semaphore::new(1));

    struct ReleaseOnDrop(#[allow(dead_code)] Option<OwnedSemaphorePermit>);

    impl ArcWake for ReleaseOnDrop {
        fn wake_by_ref(_arc_self: &Arc<Self>) {}
    }

    let mut fut = Box::pin(async {
        let _permit = sem.acquire().await.unwrap();
    });

    // Second iteration shouldn't deadlock.
    for _ in 0..=1 {
        let waker = futures::task::waker(Arc::new(ReleaseOnDrop(
            sem.clone().try_acquire_owned().ok(),
        )));
        let mut cx = std::task::Context::from_waker(&waker);
        assert!(fut.as_mut().poll(&mut cx).is_pending());
    }
}

#[test]
fn forget_permits_basic() {
    let s = Semaphore::new(10);
    assert_eq!(s.forget_permits(4), 4);
    assert_eq!(s.available_permits(), 6);
    assert_eq!(s.forget_permits(10), 6);
    assert_eq!(s.available_permits(), 0);
}

#[test]
fn update_permits_many_times() {
    let s = Semaphore::new(5);
    let mut acquire = task::spawn(s.acquire(7));
    assert_pending!(acquire.poll());
    s.release(5);
    assert_ready_ok!(acquire.poll());
    assert_eq!(s.available_permits(), 3);
    assert_eq!(s.forget_permits(3), 3);
    assert_eq!(s.available_permits(), 0);
}

use crate::sync::batch_semaphore::Priority;

#[test]
fn high_priority_overtakes_low_priority() {
    let s = Semaphore::new(1);

    // Take the only permit.
    assert_ready_ok!(task::spawn(s.acquire_priority(1, Priority::Low)).poll());

    // A low-priority waiter queues first.
    let mut low = task::spawn(s.acquire_priority(1, Priority::Low));
    assert_pending!(low.poll());

    // A high-priority waiter queues behind it.
    let mut high = task::spawn(s.acquire_priority(1, Priority::High));
    assert_pending!(high.poll());

    // The single released permit goes to the high-priority waiter.
    s.release(1);
    assert!(high.is_woken());
    assert!(!low.is_woken());
    assert_ready_ok!(high.poll());

    // The low-priority waiter gets the next permit.
    s.release(1);
    assert!(low.is_woken());
    assert_ready_ok!(low.poll());
}

#[test]
fn same_priority_remains_fifo_with_batch() {
    let s = Semaphore::new(0);

    // Two queued low-priority waiters, oldest first.
    let mut first = task::spawn(s.acquire_priority(1, Priority::Low));
    assert_pending!(first.poll());
    let mut second = task::spawn(s.acquire_priority(1, Priority::Low));
    assert_pending!(second.poll());

    // Releasing permits one at a time wakes them in arrival order.
    s.release(1);
    assert!(first.is_woken());
    assert!(!second.is_woken());
    assert_ready_ok!(first.poll());

    s.release(1);
    assert!(second.is_woken());
    assert_ready_ok!(second.poll());

    // A single batch release also wakes every waiter it can satisfy.
    let mut batch_first = task::spawn(s.acquire_priority(1, Priority::Low));
    assert_pending!(batch_first.poll());
    let mut batch_second = task::spawn(s.acquire_priority(1, Priority::Low));
    assert_pending!(batch_second.poll());
    s.release(2);
    assert!(batch_first.is_woken());
    assert!(batch_second.is_woken());
    assert_ready_ok!(batch_first.poll());
    assert_ready_ok!(batch_second.poll());
}

#[test]
fn batch_drains_high_before_low_when_all_satisfiable() {
    let s = Semaphore::new(0);

    // Queue order (oldest to newest): low, high, low.
    let mut low1 = task::spawn(s.acquire_priority(1, Priority::Low));
    assert_pending!(low1.poll());
    let mut high = task::spawn(s.acquire_priority(1, Priority::High));
    assert_pending!(high.poll());
    let mut low2 = task::spawn(s.acquire_priority(1, Priority::Low));
    assert_pending!(low2.poll());

    // Releasing one permit at a time, the high waiter overtakes the older
    // low waiter; after that, level FIFO serves low1 before low2.
    s.release(1);
    assert!(high.is_woken());
    assert!(!low1.is_woken());
    assert!(!low2.is_woken());
    assert_ready_ok!(high.poll());

    s.release(1);
    assert!(low1.is_woken());
    assert!(!low2.is_woken());
    assert_ready_ok!(low1.poll());

    s.release(1);
    assert!(low2.is_woken());
    assert_ready_ok!(low2.poll());
}

#[test]
fn low_priority_is_served_after_max_overtakes() {
    let s = Semaphore::new(0);

    // One low-priority waiter waits for a single permit.
    let mut low = task::spawn(s.acquire_priority(1, Priority::Low));
    assert_pending!(low.poll());

    // Nine high-priority waiters queue behind it.
    let mut highs = Vec::new();
    for _ in 0..9 {
        let mut high = task::spawn(s.acquire_priority(1, Priority::High));
        assert_pending!(high.poll());
        highs.push(high);
    }

    // The first eight released permits overtake the low waiter.
    for high in highs.iter_mut().take(8) {
        s.release(1);
        assert!(high.is_woken(), "high-priority waiter should overtake the low waiter");
        assert!(!low.is_woken(), "low waiter must not be woken before the limit");
        assert_ready_ok!(high.poll());
    }

    // The ninth permit is forced to the oldest (low-priority) waiter.
    s.release(1);
    assert!(low.is_woken(), "anti-starvation must serve the low-priority waiter");
    assert!(!highs[8].is_woken());
    assert_ready_ok!(low.poll());

    // The ninth high-priority waiter gets the following permit.
    s.release(1);
    assert!(highs[8].is_woken());
    assert_ready_ok!(highs[8].poll());
}

#[test]
fn large_oldest_waiter_accumulates_and_blocks_level_fifo() {
    let s = Semaphore::new(0);

    // Oldest waiter needs three permits.
    let mut big = task::spawn(s.acquire_priority(3, Priority::Low));
    assert_pending!(big.poll());

    // A newer waiter that could take a single permit must wait: within a
    // priority level, the oldest waiter is not skipped.
    let mut small = task::spawn(s.acquire_priority(1, Priority::Low));
    assert_pending!(small.poll());

    s.release(1);
    assert!(!small.is_woken());
    assert!(!big.is_woken());
    assert_eq!(s.available_permits(), 0);

    s.release(2);
    assert!(big.is_woken());
    assert!(!small.is_woken());
    assert_ready_ok!(big.poll());

    s.release(1);
    assert!(small.is_woken());
    assert_ready_ok!(small.poll());
}

#[test]
fn unsatisfiable_waiter_blocks_only_its_own_level() {
    let s = Semaphore::new(0);

    // Oldest low waiter needs two permits.
    let mut big_low = task::spawn(s.acquire_priority(2, Priority::Low));
    assert_pending!(big_low.poll());

    // A newer low waiter needing one is blocked within the low level.
    let mut small_low = task::spawn(s.acquire_priority(1, Priority::Low));
    assert_pending!(small_low.poll());

    // A newer high waiter also needing one can overtake both low waiters.
    let mut high = task::spawn(s.acquire_priority(1, Priority::High));
    assert_pending!(high.poll());

    s.release(1);
    assert!(high.is_woken());
    assert!(!big_low.is_woken());
    assert!(!small_low.is_woken());
    assert_ready_ok!(high.poll());

    // One permit cannot satisfy big_low, so the permits accumulate with it
    // and the newer small_low waiter is still blocked (level FIFO).
    s.release(1);
    assert!(!big_low.is_woken());
    assert!(!small_low.is_woken());
    assert_eq!(s.available_permits(), 0);

    // One more permit completes big_low.
    s.release(1);
    assert!(big_low.is_woken());
    assert!(!small_low.is_woken());
    assert_ready_ok!(big_low.poll());

    // small_low gets the next permit.
    s.release(1);
    assert!(small_low.is_woken());
    assert_ready_ok!(small_low.poll());
}

#[test]
fn cancelled_partially_granted_waiter_keeps_queue_progressing() {
    let s = Semaphore::new(0);

    // Oldest waiter needs three permits and accumulates one partial grant.
    let mut big = task::spawn(s.acquire_priority(3, Priority::Low));
    assert_pending!(big.poll());
    s.release(1);
    assert!(!big.is_woken());

    // A newer high waiter takes the next permit ahead of `big`.
    let mut high = task::spawn(s.acquire_priority(1, Priority::High));
    assert_pending!(high.poll());
    s.release(1);
    assert!(high.is_woken());
    assert_ready_ok!(high.poll());

    // Dropping the partially-granted waiter releases its accumulated permit.
    drop(big);
    assert_eq!(s.available_permits(), 1);

    // A fresh waiter can acquire the released permit.
    let mut next = task::spawn(s.acquire_priority(1, Priority::Low));
    assert_ready_ok!(next.poll());
}


#[test]
fn forced_low_wake_then_batch_continues_with_high() {
    let s = Semaphore::new(0);

    // Oldest low waiter.
    let mut low = task::spawn(s.acquire_priority(1, Priority::Low));
    assert_pending!(low.poll());

    // Eight high waiters exhaust the overtake budget, one release each.
    for _ in 0..8 {
        let mut high = task::spawn(s.acquire_priority(1, Priority::High));
        assert_pending!(high.poll());
        s.release(1);
        assert!(high.is_woken());
        assert!(!low.is_woken());
        assert_ready_ok!(high.poll());
    }

    // A waiting high waiter that would overtake again.
    let mut next_high = task::spawn(s.acquire_priority(1, Priority::High));
    assert_pending!(next_high.poll());

    // A batch of two permits: the first is forced to the low waiter (which
    // is removed from the queue), and the second immediately continues to the
    // high waiter within the same release.
    s.release(2);
    assert!(low.is_woken());
    assert!(next_high.is_woken());
    assert_ready_ok!(low.poll());
    assert_ready_ok!(next_high.poll());
    assert_eq!(s.available_permits(), 0);
}

#[test]
fn close_wakes_priority_waiters_with_error() {
    let s = Semaphore::new(0);

    let mut low = task::spawn(s.acquire_priority(1, Priority::Low));
    assert_pending!(low.poll());
    let mut high = task::spawn(s.acquire_priority(1, Priority::High));
    assert_pending!(high.poll());

    s.close();

    assert!(low.is_woken());
    assert!(high.is_woken());
    assert_ready_err!(low.poll());
    assert_ready_err!(high.poll());
}

