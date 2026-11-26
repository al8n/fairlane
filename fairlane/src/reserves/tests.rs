use core::{num::NonZeroUsize, task::Poll};

use crate::{
  Closed, Receiver, Sender, Single, TryReserveError,
  channel::bounded,
  testing::{Probe, poll_with},
};

fn channel_of_one() -> (Sender<u32>, Receiver<u32>) {
  bounded(NonZeroUsize::MIN).unwrap()
}

#[test]
fn a_release_grants_the_oldest_waiter() {
  let (tx, _rx) = channel_of_one();
  let first = tx.try_reserve(Single).unwrap();
  let (probe1, waker1) = Probe::new();
  let (probe2, waker2) = Probe::new();
  let mut older = tx.reserve(Single);
  let mut newer = tx.reserve(Single);
  assert!(poll_with(&mut older, &waker1).is_pending());
  assert!(poll_with(&mut newer, &waker2).is_pending());
  assert_eq!(tx.snapshot().senders_waiting(Single), 2);
  assert_eq!(tx.try_reserve(Single).unwrap_err(), TryReserveError::Full);

  drop(first);
  assert!(probe1.take());
  assert!(!probe2.take());
  assert_eq!(
    tx.try_reserve(Single).unwrap_err(),
    TryReserveError::Full,
    "the granted room is the waiter's, not free"
  );
  let Poll::Ready(Ok(permit)) = poll_with(&mut older, &waker1) else {
    panic!("the oldest waiter is granted");
  };
  assert!(poll_with(&mut newer, &waker2).is_pending());

  drop(permit);
  assert!(probe2.take());
  assert!(matches!(poll_with(&mut newer, &waker2), Poll::Ready(Ok(_))));
  assert_eq!(tx.snapshot().senders_waiting(Single), 0);
}

#[test]
fn a_dropped_waiter_passes_its_grant_on() {
  let (tx, _rx) = channel_of_one();
  let first = tx.try_reserve(Single).unwrap();
  let (_, waker1) = Probe::new();
  let (probe2, waker2) = Probe::new();
  let mut older = tx.reserve(Single);
  let mut newer = tx.reserve(Single);
  assert!(poll_with(&mut older, &waker1).is_pending());
  assert!(poll_with(&mut newer, &waker2).is_pending());
  drop(first);
  drop(older);
  assert!(probe2.take());
  assert!(matches!(poll_with(&mut newer, &waker2), Poll::Ready(Ok(_))));
}

#[test]
fn a_dropped_unnotified_waiter_leaves_the_line() {
  let (tx, _rx) = channel_of_one();
  let first = tx.try_reserve(Single).unwrap();
  let (_, waker) = Probe::new();
  let mut waiting = tx.reserve(Single);
  assert!(poll_with(&mut waiting, &waker).is_pending());
  drop(waiting);
  assert_eq!(tx.snapshot().senders_waiting(Single), 0);
  drop(first);
  assert_eq!(tx.snapshot().remaining(Single), 1);
  assert!(tx.try_reserve(Single).is_ok());
}

#[test]
fn a_close_settles_grants_not_taken_up() {
  let (tx, _rx) = channel_of_one();
  let first = tx.try_reserve(Single).unwrap();
  let (probe, waker) = Probe::new();
  let mut waiting = tx.reserve(Single);
  assert!(poll_with(&mut waiting, &waker).is_pending());
  drop(first);
  assert!(probe.take(), "granted");
  assert!(tx.close());
  assert!(matches!(
    poll_with(&mut waiting, &waker),
    Poll::Ready(Err(Closed))
  ));
  let snapshot = tx.snapshot();
  assert_eq!(
    (snapshot.remaining(Single), snapshot.reserved(Single)),
    (1, 0)
  );
}

#[test]
fn a_close_wakes_every_waiter() {
  let (tx, _rx) = channel_of_one();
  let _first = tx.try_reserve(Single).unwrap();
  let (probe1, waker1) = Probe::new();
  let (probe2, waker2) = Probe::new();
  let mut one = tx.reserve(Single);
  let mut two = tx.reserve(Single);
  assert!(poll_with(&mut one, &waker1).is_pending());
  assert!(poll_with(&mut two, &waker2).is_pending());
  tx.close();
  assert!(probe1.take() && probe2.take());
  assert!(matches!(
    poll_with(&mut one, &waker1),
    Poll::Ready(Err(Closed))
  ));
  assert!(matches!(
    poll_with(&mut two, &waker2),
    Poll::Ready(Err(Closed))
  ));
  assert_eq!(tx.snapshot().senders_waiting(Single), 0);
}
