use core::{num::NonZeroUsize, panic::AssertUnwindSafe, task::Poll};
use std::{
  panic::catch_unwind,
  sync::{Arc, atomic::Ordering::SeqCst},
};

use crate::{
  Closed, Receiver, Sender, Single, TryReserveError,
  channel::bounded,
  testing::{Faults, Probe, poll_with},
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

/// A waker whose `clone` panics, met where a waiting reserve's waker is refreshed: the reserve
/// keeps its place in line, is granted, and the lane loses no room.
#[test]
fn a_waker_clone_that_panics_loses_no_place_and_no_grant() {
  let (tx, _rx) = channel_of_one();
  let first = tx.try_reserve(Single).unwrap();
  let (probe, waker) = Probe::new();
  let mut waiting = tx.reserve(Single);
  assert!(poll_with(&mut waiting, &waker).is_pending());

  let faults = Arc::new(Faults::default());
  let faulty = faults.waker();
  faults.clone_panics.store(true, SeqCst);
  let refreshed = catch_unwind(AssertUnwindSafe(|| poll_with(&mut waiting, &faulty)));
  assert!(refreshed.is_err(), "the refresh cloned the new waker");
  let mut late = tx.reserve(Single);
  let joined = catch_unwind(AssertUnwindSafe(|| poll_with(&mut late, &faulty)));
  assert!(
    joined.is_err(),
    "a reserve about to wait clones its waker first"
  );
  faults.clone_panics.store(false, SeqCst);
  drop(late);
  assert_eq!(
    tx.snapshot().senders_waiting(Single),
    1,
    "the first reserve kept its place, and the late one never joined"
  );

  drop(first);
  assert!(probe.take(), "granted, through the waker it had");
  let Poll::Ready(Ok(permit)) = poll_with(&mut waiting, &waker) else {
    panic!("the grant is the waiting reserve's");
  };
  drop(permit);
  assert_eq!(tx.snapshot().senders_waiting(Single), 0);
  assert!(
    tx.try_reserve(Single).is_ok(),
    "the lane's one unit is free"
  );
}

/// A waker whose `drop` panics, met where a refresh replaces it and where a waiting reserve
/// leaves: each step is done before the panic, and the lane loses no room.
#[test]
fn a_waker_drop_that_panics_loses_no_place_and_no_grant() {
  let (tx, _rx) = channel_of_one();
  let first = tx.try_reserve(Single).unwrap();
  let faults = Arc::new(Faults::default());
  let faulty = faults.waker();
  let mut waiting = tx.reserve(Single);
  assert!(poll_with(&mut waiting, &faulty).is_pending());
  let mut leaving = tx.reserve(Single);
  assert!(poll_with(&mut leaving, &faulty).is_pending());

  faults.drop_panics.store(true, SeqCst);
  let (probe, waker) = Probe::new();
  let refreshed = catch_unwind(AssertUnwindSafe(|| poll_with(&mut waiting, &waker)));
  assert!(
    refreshed.is_err(),
    "the replaced waker was dropped after the lock"
  );
  let left = catch_unwind(AssertUnwindSafe(move || drop(leaving)));
  assert!(
    left.is_err(),
    "the leaving reserve's waker was dropped after it left"
  );
  faults.drop_panics.store(false, SeqCst);
  assert_eq!(tx.snapshot().senders_waiting(Single), 1);

  drop(first);
  assert!(probe.take(), "granted, through the refreshed waker");
  let Poll::Ready(Ok(permit)) = poll_with(&mut waiting, &waker) else {
    panic!("the grant is the waiting reserve's");
  };
  drop(permit);
  assert_eq!(tx.snapshot().senders_waiting(Single), 0);
  assert!(
    tx.try_reserve(Single).is_ok(),
    "the lane's one unit is free"
  );
  drop(faulty);
}

/// A waker that drops another reserve of its lane as it is woken: the release that wakes it holds
/// no lock, so the dropped reserve leaves the line at once.
#[test]
fn a_waker_may_drop_a_reserve_of_its_own_lane() {
  use std::sync::Mutex;

  struct Dropper(Mutex<Option<crate::ReserveOwned<u32, Single, crate::DefaultPolicy<Single>>>>);
  impl std::task::Wake for Dropper {
    fn wake(self: Arc<Self>) {
      drop(self.0.lock().unwrap().take());
    }
  }

  let (tx, _rx) = channel_of_one();
  let first = tx.try_reserve(Single).unwrap();
  let dropper = Arc::new(Dropper(Mutex::new(None)));
  let waker = std::task::Waker::from(dropper.clone());
  let mut woken = tx.reserve_owned(Single);
  assert!(poll_with(&mut woken, &waker).is_pending());
  let (_, idle) = Probe::new();
  let mut sibling = tx.reserve_owned(Single);
  assert!(poll_with(&mut sibling, &idle).is_pending());
  *dropper.0.lock().unwrap() = Some(sibling);

  drop(first);
  assert!(
    dropper.0.lock().unwrap().is_none(),
    "the waker dropped its sibling"
  );
  assert_eq!(tx.snapshot().senders_waiting(Single), 1);
  assert!(matches!(poll_with(&mut woken, &waker), Poll::Ready(Ok(_))));
  assert_eq!(tx.snapshot().senders_waiting(Single), 0);
}
