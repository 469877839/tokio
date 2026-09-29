use crate::sync::batch_semaphore::{Priority, Semaphore, LOW_PRIORITY_STARVATION_LIMIT};
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


#[test]
fn high_priority_woken_before_low() {
    let s = Semaphore::new(1);
    assert_ready_ok!(task::spawn(s.acquire(1)).poll());

    let mut low1 = task::spawn(s.acquire_with_priority(1, Priority::Low));
    assert_pending!(low1.poll());
    let mut low2 = task::spawn(s.acquire_with_priority(1, Priority::Low));
    assert_pending!(low2.poll());
    let mut high = task::spawn(s.acquire_with_priority(1, Priority::High));
    assert_pending!(high.poll());

    // The high-priority waiter is woken first, even though it queued last.
    s.release(1);
    assert!(high.is_woken());
    assert!(!low1.is_woken());
    assert!(!low2.is_woken());
    assert_ready_ok!(high.poll());

    // The remaining waiters keep their FIFO order.
    s.release(1);
    assert!(low1.is_woken());
    assert!(!low2.is_woken());
    assert_ready_ok!(low1.poll());

    s.release(1);
    assert!(low2.is_woken());
    assert_ready_ok!(low2.poll());
}

#[test]
fn same_priority_is_fifo() {
    let s = Semaphore::new(1);
    assert_ready_ok!(task::spawn(s.acquire(1)).poll());

    let mut high1 = task::spawn(s.acquire_with_priority(1, Priority::High));
    assert_pending!(high1.poll());
    let mut high2 = task::spawn(s.acquire_with_priority(1, Priority::High));
    assert_pending!(high2.poll());

    s.release(1);
    assert!(high1.is_woken());
    assert!(!high2.is_woken());
    assert_ready_ok!(high1.poll());

    s.release(1);
    assert!(high2.is_woken());
    assert_ready_ok!(high2.poll());
}

#[test]
fn low_priority_served_after_starvation_limit() {
    let s = Semaphore::new(1);
    assert_ready_ok!(task::spawn(s.acquire(1)).poll());

    let mut low = task::spawn(s.acquire_with_priority(1, Priority::Low));
    assert_pending!(low.poll());

    // High-priority waiters are served first...
    for round in 0..LOW_PRIORITY_STARVATION_LIMIT {
        let mut high = task::spawn(s.acquire_with_priority(1, Priority::High));
        assert_pending!(high.poll());

        s.release(1);
        assert!(high.is_woken(), "round {round}");
        assert!(!low.is_woken(), "round {round}");
        assert_ready_ok!(high.poll());
    }

    // ...but after LOW_PRIORITY_STARVATION_LIMIT consecutive high-priority
    // wakeups, the low-priority waiter goes first.
    let mut high = task::spawn(s.acquire_with_priority(1, Priority::High));
    assert_pending!(high.poll());

    s.release(1);
    assert!(low.is_woken());
    assert!(!high.is_woken());
    assert_ready_ok!(low.poll());

    // The streak resets, so the high-priority waiter is served next.
    s.release(1);
    assert!(high.is_woken());
    assert_ready_ok!(high.poll());
}

#[test]
fn starvation_fallback_feeds_low_waiter_until_satisfied() {
    let s = Semaphore::new(1);
    assert_ready_ok!(task::spawn(s.acquire(1)).poll());

    // The low-priority waiter needs two permits.
    let mut low = task::spawn(s.acquire_with_priority(2, Priority::Low));
    assert_pending!(low.poll());

    for _ in 0..LOW_PRIORITY_STARVATION_LIMIT {
        let mut high = task::spawn(s.acquire_with_priority(1, Priority::High));
        assert_pending!(high.poll());
        s.release(1);
        assert_ready_ok!(high.poll());
    }

    // The fallback has kicked in: permits now go to the low-priority waiter
    // until it is fully satisfied, even though a high-priority waiter is
    // queued.
    let mut high = task::spawn(s.acquire_with_priority(1, Priority::High));
    assert_pending!(high.poll());

    s.release(1);
    assert!(!low.is_woken());
    assert!(!high.is_woken());

    s.release(1);
    assert!(low.is_woken());
    assert!(!high.is_woken());
    assert_ready_ok!(low.poll());

    s.release(1);
    assert!(high.is_woken());
    assert_ready_ok!(high.poll());
}

#[test]
fn cancel_high_priority_waiter_keeps_low_waiters_woken() {
    let s = Semaphore::new(1);
    assert_ready_ok!(task::spawn(s.acquire(1)).poll());

    let mut low = task::spawn(s.acquire_with_priority(1, Priority::Low));
    assert_pending!(low.poll());
    let mut high = task::spawn(s.acquire_with_priority(1, Priority::High));
    assert_pending!(high.poll());

    // Dropping the queued high-priority waiter must not disturb the
    // low-priority queue.
    drop(high);

    s.release(1);
    assert!(low.is_woken());
    assert_ready_ok!(low.poll());
    assert_eq!(0, s.available_permits());
}

#[test]
fn cancel_low_priority_waiter_keeps_high_waiters_woken() {
    let s = Semaphore::new(1);
    assert_ready_ok!(task::spawn(s.acquire(1)).poll());

    let mut low = task::spawn(s.acquire_with_priority(1, Priority::Low));
    assert_pending!(low.poll());
    let mut high = task::spawn(s.acquire_with_priority(1, Priority::High));
    assert_pending!(high.poll());

    // Dropping the queued low-priority waiter must not disturb the
    // high-priority queue.
    drop(low);

    s.release(1);
    assert!(high.is_woken());
    assert_ready_ok!(high.poll());
    assert_eq!(0, s.available_permits());
}

#[test]
fn cancel_partially_assigned_low_waiter_redistributes_permits() {
    let s = Semaphore::new(2);
    assert_ready_ok!(task::spawn(s.acquire(2)).poll());

    // Low-priority waiter for 2 permits; gets one permit assigned.
    let mut low = task::spawn(s.acquire_with_priority(2, Priority::Low));
    assert_pending!(low.poll());
    s.release(1);
    assert!(!low.is_woken());
    assert_eq!(0, s.available_permits());

    // A high-priority waiter jumps ahead of the half-fed low-priority waiter.
    let mut high = task::spawn(s.acquire_with_priority(1, Priority::High));
    assert_pending!(high.poll());
    s.release(1);
    assert!(high.is_woken());
    assert_ready_ok!(high.poll());

    // Dropping the low-priority waiter returns its assigned permit to the
    // semaphore.
    drop(low);
    assert_eq!(1, s.available_permits());
}

#[test]
fn close_semaphore_notifies_all_priorities() {
    let s = Semaphore::new(1);
    assert_ready_ok!(task::spawn(s.acquire(1)).poll());

    let mut low = task::spawn(s.acquire_with_priority(1, Priority::Low));
    assert_pending!(low.poll());
    let mut high = task::spawn(s.acquire_with_priority(1, Priority::High));
    assert_pending!(high.poll());

    s.close();

    assert!(low.is_woken());
    assert!(high.is_woken());
    assert_ready_err!(low.poll());
    assert_ready_err!(high.poll());
}
