#![warn(rust_2018_idioms)]

use tokio::pin;
use tokio::sync::oneshot;
use tokio_util::sync::{CancellationToken, WaitForCancellationFuture};

use core::future::Future;
use core::hash::Hash;
use core::task::{Context, Poll};
use futures_test::task::new_count_waker;
use std::hash::{DefaultHasher, Hasher};

#[test]
fn cancel_token() {
    let (waker, wake_counter) = new_count_waker();
    let token = CancellationToken::new();
    assert!(!token.is_cancelled());

    let wait_fut = token.cancelled();
    pin!(wait_fut);

    assert_eq!(
        Poll::Pending,
        wait_fut.as_mut().poll(&mut Context::from_waker(&waker))
    );
    assert_eq!(wake_counter, 0);

    let wait_fut_2 = token.cancelled();
    pin!(wait_fut_2);

    token.cancel();
    assert_eq!(wake_counter, 1);
    assert!(token.is_cancelled());

    assert_eq!(
        Poll::Ready(()),
        wait_fut.as_mut().poll(&mut Context::from_waker(&waker))
    );
    assert_eq!(
        Poll::Ready(()),
        wait_fut_2.as_mut().poll(&mut Context::from_waker(&waker))
    );
}

#[test]
fn cancel_token_with_reason() {
    let (waker, wake_counter) = new_count_waker();
    let token = CancellationToken::new();
    assert!(token.cancellation_reason().is_none());

    let wait_fut = token.cancelled();
    pin!(wait_fut);
    assert_eq!(
        Poll::Pending,
        wait_fut.as_mut().poll(&mut Context::from_waker(&waker))
    );

    token.cancel_with_reason("shutting down");
    assert_eq!(wake_counter, 1);
    assert!(token.is_cancelled());

    // The reason is visible as soon as the token is observed to be cancelled.
    let reason = token.cancellation_reason().unwrap();
    assert_eq!(Some(&"shutting down"), reason.downcast_ref::<&str>());

    // Waiters that were already attached get woken up.
    assert_eq!(
        Poll::Ready(()),
        wait_fut.as_mut().poll(&mut Context::from_waker(&waker))
    );

    // Waiters that attach only after the cancellation still complete and
    // still see the reason.
    let late_fut = token.cancelled();
    pin!(late_fut);
    assert_eq!(
        Poll::Ready(()),
        late_fut.as_mut().poll(&mut Context::from_waker(&waker))
    );
    let reason = token.cancellation_reason().unwrap();
    assert_eq!(Some(&"shutting down"), reason.downcast_ref::<&str>());
}

#[test]
fn cancel_without_reason_stores_unit_reason() {
    let token = CancellationToken::new();
    token.cancel();

    let reason = token.cancellation_reason().unwrap();
    assert_eq!(Some(&()), reason.downcast_ref::<()>());
}

#[test]
fn first_cancellation_reason_wins() {
    let token = CancellationToken::new();
    token.cancel_with_reason(1_u32);
    token.cancel_with_reason(2_u32);
    token.cancel();

    let reason = token.cancellation_reason().unwrap();
    assert_eq!(Some(&1_u32), reason.downcast_ref::<u32>());
}

#[test]
fn cancellation_reason_is_shared_with_clones_and_children() {
    let token = CancellationToken::new();
    let cloned_token = token.clone();
    let child_token = token.child_token();
    let grandchild_token = child_token.child_token();

    token.cancel_with_reason(String::from("parent says stop"));

    for token in [&token, &cloned_token, &child_token, &grandchild_token] {
        assert!(token.is_cancelled());
        let reason = token.cancellation_reason().unwrap();
        assert_eq!(
            Some(&String::from("parent says stop")),
            reason.downcast_ref::<String>()
        );
    }

    // Children created after the cancellation inherit the reason as well.
    let late_child = token.child_token();
    assert!(late_child.is_cancelled());
    let reason = late_child.cancellation_reason().unwrap();
    assert_eq!(
        Some(&String::from("parent says stop")),
        reason.downcast_ref::<String>()
    );
}

#[test]
fn cancellation_reason_survives_dropped_intermediate_tokens() {
    let token = CancellationToken::new();
    let intermediate_token = token.child_token();
    let child_token = intermediate_token.child_token();
    drop(intermediate_token);

    token.cancel_with_reason(7_u8);

    assert!(child_token.is_cancelled());
    let reason = child_token.cancellation_reason().unwrap();
    assert_eq!(Some(&7_u8), reason.downcast_ref::<u8>());
}

#[test]
fn child_cancelled_directly_keeps_its_own_reason() {
    let token = CancellationToken::new();
    let child_token = token.child_token();
    let sibling_token = token.child_token();

    child_token.cancel_with_reason("child reason");
    token.cancel_with_reason("parent reason");

    // The child was cancelled first, so its reason is kept.
    let reason = child_token.cancellation_reason().unwrap();
    assert_eq!(Some(&"child reason"), reason.downcast_ref::<&str>());

    // The sibling gets cancelled by the parent and sees the parent's reason.
    let reason = sibling_token.cancellation_reason().unwrap();
    assert_eq!(Some(&"parent reason"), reason.downcast_ref::<&str>());

    // The parent itself is not cancelled by its children.
    let reason = token.cancellation_reason().unwrap();
    assert_eq!(Some(&"parent reason"), reason.downcast_ref::<&str>());
}

#[test]
fn cancellation_reason_is_visible_to_owned_waiters() {
    let token = CancellationToken::new();
    let wait_fut = token.clone().cancelled_owned();
    pin!(wait_fut);

    token.cancel_with_reason(3.5_f64);

    let (waker, _) = new_count_waker();
    assert_eq!(
        Poll::Ready(()),
        wait_fut.as_mut().poll(&mut Context::from_waker(&waker))
    );
    let reason = token.cancellation_reason().unwrap();
    assert_eq!(Some(&3.5_f64), reason.downcast_ref::<f64>());
}

#[test]
fn concurrent_cancellation_reason_first_wins() {
    for _ in 0..100 {
        let token = CancellationToken::new();
        let child_token = token.child_token();

        std::thread::scope(|scope| {
            for i in 0..8_u32 {
                let token = token.clone();
                scope.spawn(move || {
                    token.cancel_with_reason(i);
                });
            }
        });

        // Exactly one reason won the race, and every observer agrees on it.
        let reason = *token
            .cancellation_reason()
            .unwrap()
            .downcast_ref::<u32>()
            .unwrap();
        assert!(reason < 8);
        assert_eq!(
            Some(&reason),
            child_token
                .cancellation_reason()
                .unwrap()
                .downcast_ref::<u32>()
        );

        // A child created after the cancellation sees the same reason.
        let late_child = token.child_token();
        assert_eq!(
            Some(&reason),
            late_child
                .cancellation_reason()
                .unwrap()
                .downcast_ref::<u32>()
        );
    }
}

#[test]
fn cancel_token_owned() {
    let (waker, wake_counter) = new_count_waker();
    let token = CancellationToken::new();
    assert!(!token.is_cancelled());

    let wait_fut = token.clone().cancelled_owned();
    pin!(wait_fut);

    assert_eq!(
        Poll::Pending,
        wait_fut.as_mut().poll(&mut Context::from_waker(&waker))
    );
    assert_eq!(wake_counter, 0);

    let wait_fut_2 = token.clone().cancelled_owned();
    pin!(wait_fut_2);

    token.cancel();
    assert_eq!(wake_counter, 1);
    assert!(token.is_cancelled());

    assert_eq!(
        Poll::Ready(()),
        wait_fut.as_mut().poll(&mut Context::from_waker(&waker))
    );
    assert_eq!(
        Poll::Ready(()),
        wait_fut_2.as_mut().poll(&mut Context::from_waker(&waker))
    );
}

#[test]
fn cancel_token_owned_drop_test() {
    let (waker, wake_counter) = new_count_waker();
    let token = CancellationToken::new();

    let future = token.cancelled_owned();
    pin!(future);

    assert_eq!(
        Poll::Pending,
        future.as_mut().poll(&mut Context::from_waker(&waker))
    );
    assert_eq!(wake_counter, 0);

    // let future be dropped while pinned and under pending state to
    // find potential memory related bugs.
}

#[test]
fn cancel_child_token_through_parent() {
    let (waker, wake_counter) = new_count_waker();
    let token = CancellationToken::new();

    let child_token = token.child_token();
    assert!(!child_token.is_cancelled());

    let child_fut = child_token.cancelled();
    pin!(child_fut);
    let parent_fut = token.cancelled();
    pin!(parent_fut);

    assert_eq!(
        Poll::Pending,
        child_fut.as_mut().poll(&mut Context::from_waker(&waker))
    );
    assert_eq!(
        Poll::Pending,
        parent_fut.as_mut().poll(&mut Context::from_waker(&waker))
    );
    assert_eq!(wake_counter, 0);

    token.cancel();
    assert_eq!(wake_counter, 2);
    assert!(token.is_cancelled());
    assert!(child_token.is_cancelled());

    assert_eq!(
        Poll::Ready(()),
        child_fut.as_mut().poll(&mut Context::from_waker(&waker))
    );
    assert_eq!(
        Poll::Ready(()),
        parent_fut.as_mut().poll(&mut Context::from_waker(&waker))
    );
}

#[test]
fn cancel_grandchild_token_through_parent_if_child_was_dropped() {
    let (waker, wake_counter) = new_count_waker();
    let token = CancellationToken::new();

    let intermediate_token = token.child_token();
    let child_token = intermediate_token.child_token();
    drop(intermediate_token);
    assert!(!child_token.is_cancelled());

    let child_fut = child_token.cancelled();
    pin!(child_fut);
    let parent_fut = token.cancelled();
    pin!(parent_fut);

    assert_eq!(
        Poll::Pending,
        child_fut.as_mut().poll(&mut Context::from_waker(&waker))
    );
    assert_eq!(
        Poll::Pending,
        parent_fut.as_mut().poll(&mut Context::from_waker(&waker))
    );
    assert_eq!(wake_counter, 0);

    token.cancel();
    assert_eq!(wake_counter, 2);
    assert!(token.is_cancelled());
    assert!(child_token.is_cancelled());

    assert_eq!(
        Poll::Ready(()),
        child_fut.as_mut().poll(&mut Context::from_waker(&waker))
    );
    assert_eq!(
        Poll::Ready(()),
        parent_fut.as_mut().poll(&mut Context::from_waker(&waker))
    );
}

#[test]
fn cancel_child_token_without_parent() {
    let (waker, wake_counter) = new_count_waker();
    let token = CancellationToken::new();

    let child_token_1 = token.child_token();

    let child_fut = child_token_1.cancelled();
    pin!(child_fut);
    let parent_fut = token.cancelled();
    pin!(parent_fut);

    assert_eq!(
        Poll::Pending,
        child_fut.as_mut().poll(&mut Context::from_waker(&waker))
    );
    assert_eq!(
        Poll::Pending,
        parent_fut.as_mut().poll(&mut Context::from_waker(&waker))
    );
    assert_eq!(wake_counter, 0);

    child_token_1.cancel();
    assert_eq!(wake_counter, 1);
    assert!(!token.is_cancelled());
    assert!(child_token_1.is_cancelled());

    assert_eq!(
        Poll::Ready(()),
        child_fut.as_mut().poll(&mut Context::from_waker(&waker))
    );
    assert_eq!(
        Poll::Pending,
        parent_fut.as_mut().poll(&mut Context::from_waker(&waker))
    );

    let child_token_2 = token.child_token();
    let child_fut_2 = child_token_2.cancelled();
    pin!(child_fut_2);

    assert_eq!(
        Poll::Pending,
        child_fut_2.as_mut().poll(&mut Context::from_waker(&waker))
    );
    assert_eq!(
        Poll::Pending,
        parent_fut.as_mut().poll(&mut Context::from_waker(&waker))
    );

    token.cancel();
    assert_eq!(wake_counter, 3);
    assert!(token.is_cancelled());
    assert!(child_token_2.is_cancelled());

    assert_eq!(
        Poll::Ready(()),
        child_fut_2.as_mut().poll(&mut Context::from_waker(&waker))
    );
    assert_eq!(
        Poll::Ready(()),
        parent_fut.as_mut().poll(&mut Context::from_waker(&waker))
    );
}

#[test]
fn create_child_token_after_parent_was_cancelled() {
    for drop_child_first in [true, false].iter().cloned() {
        let (waker, wake_counter) = new_count_waker();
        let token = CancellationToken::new();
        token.cancel();

        let child_token = token.child_token();
        assert!(child_token.is_cancelled());

        {
            let child_fut = child_token.cancelled();
            pin!(child_fut);
            let parent_fut = token.cancelled();
            pin!(parent_fut);

            assert_eq!(
                Poll::Ready(()),
                child_fut.as_mut().poll(&mut Context::from_waker(&waker))
            );
            assert_eq!(
                Poll::Ready(()),
                parent_fut.as_mut().poll(&mut Context::from_waker(&waker))
            );
            assert_eq!(wake_counter, 0);
        }

        if drop_child_first {
            drop(child_token);
            drop(token);
        } else {
            drop(token);
            drop(child_token);
        }
    }
}

#[test]
fn drop_multiple_child_tokens() {
    for drop_first_child_first in &[true, false] {
        let token = CancellationToken::new();
        let mut child_tokens = [None, None, None];
        for child in &mut child_tokens {
            *child = Some(token.child_token());
        }

        assert!(!token.is_cancelled());
        assert!(!child_tokens[0].as_ref().unwrap().is_cancelled());

        for i in 0..child_tokens.len() {
            if *drop_first_child_first {
                child_tokens[i] = None;
            } else {
                child_tokens[child_tokens.len() - 1 - i] = None;
            }
            assert!(!token.is_cancelled());
        }

        drop(token);
    }
}

#[test]
fn cancel_only_all_descendants() {
    // ARRANGE
    let (waker, wake_counter) = new_count_waker();

    let parent_token = CancellationToken::new();
    let token = parent_token.child_token();
    let sibling_token = parent_token.child_token();
    let child1_token = token.child_token();
    let child2_token = token.child_token();
    let grandchild_token = child1_token.child_token();
    let grandchild2_token = child1_token.child_token();
    let great_grandchild_token = grandchild_token.child_token();

    assert!(!parent_token.is_cancelled());
    assert!(!token.is_cancelled());
    assert!(!sibling_token.is_cancelled());
    assert!(!child1_token.is_cancelled());
    assert!(!child2_token.is_cancelled());
    assert!(!grandchild_token.is_cancelled());
    assert!(!grandchild2_token.is_cancelled());
    assert!(!great_grandchild_token.is_cancelled());

    let parent_fut = parent_token.cancelled();
    let fut = token.cancelled();
    let sibling_fut = sibling_token.cancelled();
    let child1_fut = child1_token.cancelled();
    let child2_fut = child2_token.cancelled();
    let grandchild_fut = grandchild_token.cancelled();
    let grandchild2_fut = grandchild2_token.cancelled();
    let great_grandchild_fut = great_grandchild_token.cancelled();

    pin!(parent_fut);
    pin!(fut);
    pin!(sibling_fut);
    pin!(child1_fut);
    pin!(child2_fut);
    pin!(grandchild_fut);
    pin!(grandchild2_fut);
    pin!(great_grandchild_fut);

    assert_eq!(
        Poll::Pending,
        parent_fut.as_mut().poll(&mut Context::from_waker(&waker))
    );
    assert_eq!(
        Poll::Pending,
        fut.as_mut().poll(&mut Context::from_waker(&waker))
    );
    assert_eq!(
        Poll::Pending,
        sibling_fut.as_mut().poll(&mut Context::from_waker(&waker))
    );
    assert_eq!(
        Poll::Pending,
        child1_fut.as_mut().poll(&mut Context::from_waker(&waker))
    );
    assert_eq!(
        Poll::Pending,
        child2_fut.as_mut().poll(&mut Context::from_waker(&waker))
    );
    assert_eq!(
        Poll::Pending,
        grandchild_fut
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
    );
    assert_eq!(
        Poll::Pending,
        grandchild2_fut
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
    );
    assert_eq!(
        Poll::Pending,
        great_grandchild_fut
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
    );
    assert_eq!(wake_counter, 0);

    // ACT
    token.cancel();

    // ASSERT
    assert_eq!(wake_counter, 6);
    assert!(!parent_token.is_cancelled());
    assert!(token.is_cancelled());
    assert!(!sibling_token.is_cancelled());
    assert!(child1_token.is_cancelled());
    assert!(child2_token.is_cancelled());
    assert!(grandchild_token.is_cancelled());
    assert!(grandchild2_token.is_cancelled());
    assert!(great_grandchild_token.is_cancelled());

    assert_eq!(
        Poll::Ready(()),
        fut.as_mut().poll(&mut Context::from_waker(&waker))
    );
    assert_eq!(
        Poll::Ready(()),
        child1_fut.as_mut().poll(&mut Context::from_waker(&waker))
    );
    assert_eq!(
        Poll::Ready(()),
        child2_fut.as_mut().poll(&mut Context::from_waker(&waker))
    );
    assert_eq!(
        Poll::Ready(()),
        grandchild_fut
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
    );
    assert_eq!(
        Poll::Ready(()),
        grandchild2_fut
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
    );
    assert_eq!(
        Poll::Ready(()),
        great_grandchild_fut
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
    );
    assert_eq!(wake_counter, 6);
}

#[test]
fn drop_parent_before_child_tokens() {
    let token = CancellationToken::new();
    let child1 = token.child_token();
    let child2 = token.child_token();

    drop(token);
    assert!(!child1.is_cancelled());

    drop(child1);
    drop(child2);
}

#[test]
fn derives_send_sync() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<CancellationToken>();
    assert_sync::<CancellationToken>();

    assert_send::<WaitForCancellationFuture<'static>>();
    assert_sync::<WaitForCancellationFuture<'static>>();
}

#[test]
fn run_until_cancelled_test() {
    let (waker, _) = new_count_waker();

    {
        let token = CancellationToken::new();

        let fut = token.run_until_cancelled(std::future::pending::<()>());
        pin!(fut);

        assert_eq!(
            Poll::Pending,
            fut.as_mut().poll(&mut Context::from_waker(&waker))
        );

        token.cancel();

        assert_eq!(
            Poll::Ready(None),
            fut.as_mut().poll(&mut Context::from_waker(&waker))
        );
    }

    {
        let (tx, rx) = oneshot::channel::<()>();

        let token = CancellationToken::new();
        let fut = token.run_until_cancelled(async move {
            rx.await.unwrap();
            42
        });
        pin!(fut);

        assert_eq!(
            Poll::Pending,
            fut.as_mut().poll(&mut Context::from_waker(&waker))
        );

        tx.send(()).unwrap();

        assert_eq!(
            Poll::Ready(Some(42)),
            fut.as_mut().poll(&mut Context::from_waker(&waker))
        );
    }

    // Do not poll the future when token is already cancelled.
    {
        let token = CancellationToken::new();

        let fut = token.run_until_cancelled(async { panic!("fut polled after cancellation") });
        pin!(fut);

        token.cancel();

        assert_eq!(
            Poll::Ready(None),
            fut.as_mut().poll(&mut Context::from_waker(&waker))
        );
    }
}

#[test]
fn run_until_cancelled_owned_test() {
    let (waker, _) = new_count_waker();

    {
        let token = CancellationToken::new();
        let to_cancel = token.clone();

        let takes_ownership = move |token: CancellationToken| {
            token.run_until_cancelled_owned(std::future::pending::<()>())
        };

        let fut = takes_ownership(token);
        pin!(fut);

        assert_eq!(
            Poll::Pending,
            fut.as_mut().poll(&mut Context::from_waker(&waker))
        );

        to_cancel.cancel();

        assert_eq!(
            Poll::Ready(None),
            fut.as_mut().poll(&mut Context::from_waker(&waker))
        );
    }

    {
        let (tx, rx) = oneshot::channel::<()>();

        let token = CancellationToken::new();
        let takes_ownership = move |token: CancellationToken, rx: oneshot::Receiver<()>| {
            token.run_until_cancelled_owned(async move {
                rx.await.unwrap();
                42
            })
        };
        let fut = takes_ownership(token, rx);
        pin!(fut);

        assert_eq!(
            Poll::Pending,
            fut.as_mut().poll(&mut Context::from_waker(&waker))
        );

        tx.send(()).unwrap();

        assert_eq!(
            Poll::Ready(Some(42)),
            fut.as_mut().poll(&mut Context::from_waker(&waker))
        );
    }
}

#[test]
fn cloned_cancellation_tokens_are_considered_equal() {
    let token = CancellationToken::new();
    let token_clone = token.clone();

    assert_eq!(token, token_clone);
}

#[test]
fn child_cancellation_tokens_are_not_considered_equal() {
    let token = CancellationToken::new();
    let token_clone = token.child_token();

    assert_ne!(token, token_clone);
}

#[test]
fn independent_cancellation_tokens_are_not_considered_equal() {
    let token1 = CancellationToken::new();
    let token2 = CancellationToken::new();

    assert_ne!(token1, token2);
}

#[test]
fn cloned_cancellation_tokens_have_same_hash() {
    let token1 = CancellationToken::new();
    let token2 = token1.clone();

    let mut state1 = DefaultHasher::default();
    token1.hash(&mut state1);
    let mut state2 = DefaultHasher::default();
    token2.hash(&mut state2);
    assert_eq!(state1.finish(), state2.finish());
}

#[test]
fn different_cancellation_tokens_have_different_hash() {
    let token1 = CancellationToken::new();
    let token2 = CancellationToken::new();

    let mut state1 = DefaultHasher::default();
    token1.hash(&mut state1);
    let mut state2 = DefaultHasher::default();
    token2.hash(&mut state2);
    assert_ne!(state1.finish(), state2.finish());
}
