//! Laws for callers' code that breaks its contract: a hand-written `Lane` whose answers change, and
//! the like. The channel may answer wrongly for such a caller, but it never writes to a lane it
//! holds no room in, and loses no item and no room.

use core::{
  panic::AssertUnwindSafe,
  sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::panic::catch_unwind;

use super::*;
use crate::{
  LaneView, Policy, Strict,
  lane::Lane,
  testing::{Faults, Three},
};

/// A lane whose `index` answers from a switch: it passes the round trip at construction and
/// changes its answer later.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fickle {
  A,
  B,
}

static FLIPPED: AtomicBool = AtomicBool::new(false);

impl Lane for Fickle {
  type N = typenum::U2;

  fn index(self) -> usize {
    let index = self as usize;
    if FLIPPED.load(Ordering::SeqCst) {
      1 - index
    } else {
      index
    }
  }

  fn from_index(index: usize) -> Option<Self> {
    [Self::A, Self::B].get(index).copied()
  }

  fn as_str(self) -> &'static str {
    match self {
      Self::A => "a",
      Self::B => "b",
    }
  }
}

/// A permit commits to, and refunds, the lane its room was reserved in, whatever its `Lane` says
/// at the commit.
#[test]
fn a_permit_keeps_the_lane_it_reserved() {
  let options = Options::from_fn(|_| LaneOptions::new(cap(1)));
  let (tx, rx) = channel_with::<u32, Fickle, Strict>(options, Strict).unwrap();

  let sent = tx.try_reserve(Fickle::A).unwrap();
  let dropped = tx.try_reserve(Fickle::B).unwrap();
  FLIPPED.store(true, Ordering::SeqCst);
  sent.send(7).unwrap();
  drop(dropped);
  FLIPPED.store(false, Ordering::SeqCst);

  let snapshot = tx.snapshot();
  assert_eq!(
    (snapshot.queued(Fickle::A), snapshot.queued(Fickle::B)),
    (1, 0),
    "the item is in the lane the permit reserved"
  );
  assert_eq!(
    (snapshot.remaining(Fickle::A), snapshot.remaining(Fickle::B)),
    (0, 1),
    "the dropped permit's room went back to its own lane"
  );
  let received = rx.try_recv().unwrap().unwrap();
  assert_eq!(
    (received.lane(), received.ordinal(), received.into_item()),
    (Fickle::A, 0, 7)
  );
}

/// A stamp policy whose `stamp` panics while armed, and counts its calls.
#[derive(Default)]
struct Stamping {
  armed: AtomicBool,
  stamps: AtomicUsize,
}

impl Policy<Three> for Stamping {
  type Stamp = u64;

  fn stamp(&self) -> u64 {
    self.stamps.fetch_add(1, Ordering::SeqCst);
    if self.armed.load(Ordering::SeqCst) {
      panic!("a stamp that panics");
    }
    0
  }

  fn owed(&self, _: &LaneView<Three, u64>) -> Option<Three> {
    None
  }
}

fn stamped() -> (Sender<u32, Three, Stamping>, Receiver<u32, Three, Stamping>) {
  let options = Options::from_fn(|_| LaneOptions::new(cap(1)));
  channel_with(options, Stamping::default()).unwrap()
}

/// A `stamp` that panics during a permit's send gives the permit's room back: nothing was
/// committed, and the lane's one unit is free again.
#[test]
fn a_stamp_that_panics_gives_the_room_back() {
  let (tx, _rx) = stamped();
  let permit = tx.try_reserve(Three::B).unwrap();
  tx.policy().armed.store(true, Ordering::SeqCst);
  let sent = catch_unwind(AssertUnwindSafe(move || permit.send(1)));
  tx.policy().armed.store(false, Ordering::SeqCst);
  assert!(sent.is_err(), "the stamp panicked");
  let snapshot = tx.snapshot();
  assert_eq!(
    (snapshot.queued(Three::B), snapshot.remaining(Three::B)),
    (0, 1)
  );
  assert!(tx.try_reserve(Three::B).is_ok());
}

/// A permit sent after the channel closed hands its item back without calling `stamp`.
#[test]
fn a_send_after_the_close_calls_no_stamp() {
  let (tx, _rx) = stamped();
  let permit = tx.try_reserve(Three::B).unwrap();
  assert!(tx.close());
  tx.policy().armed.store(true, Ordering::SeqCst);
  assert_eq!(permit.send(5), Err(SendError(5)));
  assert_eq!(tx.policy().stamps.load(Ordering::SeqCst), 0);
}

/// A lane whose `==` panics.
#[derive(Debug, Clone, Copy)]
enum Touchy {
  A,
  B,
}

impl PartialEq for Touchy {
  fn eq(&self, _: &Self) -> bool {
    panic!("a lane's `==` panics")
  }
}

impl Eq for Touchy {}

impl Lane for Touchy {
  type N = typenum::U2;

  fn index(self) -> usize {
    self as usize
  }

  fn from_index(index: usize) -> Option<Self> {
    [Self::A, Self::B].get(index).copied()
  }

  fn as_str(self) -> &'static str {
    match self {
      Self::A => "a",
      Self::B => "b",
    }
  }
}

/// With two lanes in view, a receive's choice and its passed-over counts compare no lanes: a lane
/// whose `==` panics costs no item and no room.
#[test]
fn a_receive_never_compares_lanes() {
  let options = Options::from_fn(|_| LaneOptions::new(cap(2)));
  let (tx, rx) = channel_with::<u32, Touchy, Strict>(options, Strict).unwrap();
  tx.try_send(Touchy::B, 1).unwrap();
  tx.try_send(Touchy::A, 2).unwrap();
  let mut batch = Vec::new();
  assert_eq!(rx.try_recv_many(&mut batch, 2), Ok(2));
  let items: Vec<u32> = batch.iter().map(|received| *received.item()).collect();
  assert_eq!(items, [2, 1]);
  let snapshot = tx.snapshot();
  assert_eq!(
    (snapshot.remaining(Touchy::A), snapshot.remaining(Touchy::B)),
    (2, 2)
  );
}

/// On a channel of one lane, the reserve granted an item's room is woken only once the item is in
/// the caller's batch: a waker that panics there costs no item, and the grant stands.
#[test]
fn a_waker_that_panics_costs_a_batch_no_item() {
  let (tx, rx) = bounded::<u32>(cap(1)).unwrap();
  tx.try_send(Single, 7).unwrap();
  let faults = StdArc::new(Faults::default());
  let faulty = faults.waker();
  let mut waiting = tx.reserve(Single);
  assert!(poll_with(&mut waiting, &faulty).is_pending());

  faults.wake_panics.store(true, SeqCst);
  let mut batch = Vec::new();
  let unwound = catch_unwind(AssertUnwindSafe(|| rx.try_recv_many(&mut batch, 1)));
  faults.wake_panics.store(false, SeqCst);
  assert!(unwound.is_err(), "the granted reserve's waker panicked");
  let items: Vec<u32> = batch.iter().map(|received| *received.item()).collect();
  assert_eq!(items, [7], "the item is the caller's");
  assert!(
    matches!(poll_with(&mut waiting, &faulty), Poll::Ready(Ok(_))),
    "the grant stands"
  );
}
