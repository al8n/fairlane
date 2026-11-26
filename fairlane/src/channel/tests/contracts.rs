//! Laws for callers' code that breaks its contract: a hand-written `Lane` whose answers change, and
//! the like. The channel may answer wrongly for such a caller, but it never writes to a lane it
//! holds no room in, and loses no item and no room.

use core::sync::atomic::{AtomicBool, Ordering};

use super::*;
use crate::{Strict, lane::Lane};

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
