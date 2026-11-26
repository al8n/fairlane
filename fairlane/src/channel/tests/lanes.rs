//! Laws of a channel of several lanes: N1-N3, X4, G4 and X3.

use core::{
  num::{NonZeroU32, NonZeroUsize},
  sync::atomic::{AtomicUsize, Ordering},
};

use super::*;
use crate::{LaneView, PassLimit, Policy, Quota, Received, Strict, lane::Lane, testing::Three};

/// Three lanes of `capacity`; lane A express where `express`.
fn three(capacity: usize, express: bool) -> Options<Three> {
  Options::from_fn(|lane| {
    let options = LaneOptions::new(cap(capacity));
    if lane == Three::A {
      options.with_express(express)
    } else {
      options
    }
  })
}

fn lanes_of(batch: &[Received<u32, Three>]) -> Vec<Three> {
  batch.iter().map(Received::lane).collect()
}

#[test]
fn lanes_refuse_malformed_options() {
  let behind = three(1, false).with_lane(Three::C, LaneOptions::new(cap(1)).with_express(true));
  assert_eq!(
    channel::<u8, Three>(behind).unwrap_err(),
    Malformed::ExpressBehind(2)
  );
  let huge = three(1, false).with_lane(Three::B, LaneOptions::new(NonZeroUsize::MAX));
  assert_eq!(
    channel::<u8, Three>(huge).unwrap_err(),
    Malformed::Capacity(1)
  );
}

#[test]
fn lanes_serve_in_priority_order() {
  let (tx, rx) = channel_with::<u32, Three, _>(three(4, false), Strict).unwrap();
  tx.try_send(Three::C, 3).unwrap();
  tx.try_send(Three::B, 2).unwrap();
  tx.try_send(Three::A, 1).unwrap();
  let mut batch = Vec::new();
  assert_eq!(rx.try_recv_many(&mut batch, 8), Ok(3));
  assert_eq!(lanes_of(&batch), [Three::A, Three::B, Three::C]);
}

#[test]
fn lanes_serve_an_express_lane_first() {
  let (tx, rx) = channel_with::<u32, Three, _>(three(4, true), Strict).unwrap();
  tx.try_send(Three::B, 2).unwrap();
  tx.try_send(Three::A, 1).unwrap();
  let first = rx.try_recv().unwrap().unwrap();
  assert_eq!(first.lane(), Three::A);
}

#[test]
fn lanes_admit_only_the_quota_range() {
  let (tx, rx) = channel_with::<u32, Three, _>(three(4, false), Strict).unwrap();
  tx.try_send(Three::A, 1).unwrap();
  tx.try_send(Three::C, 3).unwrap();
  let mut batch = Vec::new();
  assert_eq!(
    rx.try_recv_many(&mut batch, Quota::up_to(8).lanes(Three::B..)),
    Ok(1)
  );
  assert_eq!(lanes_of(&batch), [Three::C]);
  assert_eq!(tx.snapshot().queued(Three::A), 1);
}

#[test]
fn lanes_ordinals_are_per_lane() {
  let (tx, rx) = channel_with::<u32, Three, _>(three(8, false), Strict).unwrap();
  let other = rx.clone();
  for round in 0..4 {
    tx.try_send(Three::B, round).unwrap();
    tx.try_send(Three::C, round).unwrap();
  }
  let mut next = [0_u64; 3];
  for turn in 0..8 {
    let receiver = if turn % 2 == 0 { &rx } else { &other };
    let received = receiver.try_recv().unwrap().unwrap();
    let index = received.lane().index();
    assert_eq!(received.ordinal(), next[index]);
    next[index] += 1;
  }
  assert_eq!(next, [0, 4, 4]);
}

/// Owes `owed` once its passed-over count reaches `limit`; every other lane may starve.
fn owe(owed: Three, limit: u32) -> PassLimit<Three> {
  Three::ALL
    .iter()
    .fold(PassLimit::new(NonZeroU32::MIN), |policy, &lane| {
      policy.with_lane(
        lane,
        (lane == owed).then(|| NonZeroU32::new(limit).unwrap()),
      )
    })
}

/// X4, gate condition 1: a batch stops at a lane's limit and never passes the lane over.
#[test]
fn x4_a_batch_stops_at_a_lane_limit() {
  let (tx, rx) = channel_with::<u32, Three, _>(three(64, false), owe(Three::B, 2)).unwrap();
  for item in 0..64 {
    tx.try_send(Three::A, item).unwrap();
    tx.try_send(Three::B, 100 + item).unwrap();
  }
  let mut batch = Vec::new();
  for _ in 0..2 {
    assert_eq!(rx.try_recv_many(&mut batch, 1), Ok(1));
  }
  assert_eq!(lanes_of(&batch), [Three::A, Three::A]);
  batch.clear();
  let quota = Quota::up_to(32)
    .limit_with(Three::A, 1)
    .limit_with(Three::B, 32);
  assert_eq!(rx.try_recv_many(&mut batch, &quota), Ok(1));
  assert_eq!(lanes_of(&batch), [Three::B], "the batch stops at A's limit");
  batch.clear();
  assert_eq!(rx.try_recv_many(&mut batch, &quota), Ok(1));
  assert_eq!(lanes_of(&batch), [Three::A]);
}

/// Panics on its second `owed`; counts its calls.
struct PanicsOnSecond(AtomicUsize);

impl Policy<Three> for PanicsOnSecond {
  type Stamp = ();

  fn stamp(&self) {}

  fn owed(&self, _: &LaneView<Three, ()>) -> Option<Three> {
    if self.0.fetch_add(1, Ordering::SeqCst) == 1 {
      panic!("a policy that panics");
    }
    None
  }
}

/// G4, gate condition 4: a policy that panics inside a batch loses no item and leaks no room.
#[test]
fn g4_a_panicking_policy_loses_no_item() {
  let policy = PanicsOnSecond(AtomicUsize::new(0));
  let (tx, rx) = channel_with::<u32, Three, _>(three(4, false), policy).unwrap();
  for item in 0..3 {
    tx.try_send(Three::B, item).unwrap();
    tx.try_send(Three::C, 10 + item).unwrap();
  }
  let mut batch = Vec::new();
  let hook = std::panic::take_hook();
  std::panic::set_hook(Box::new(|_| {}));
  let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
    rx.try_recv_many(&mut batch, 8)
  }));
  std::panic::set_hook(hook);
  assert!(unwound.is_err(), "the policy panicked on its second call");
  assert_eq!(
    lanes_of(&batch),
    [Three::B],
    "the item taken before the panic is the caller's"
  );
  let snapshot = tx.snapshot();
  assert_eq!(snapshot.remaining(Three::B), 2, "its room is back");
  assert_eq!(snapshot.queued(Three::B) + snapshot.queued(Three::C), 5);
  batch.clear();
  assert_eq!(
    rx.try_recv_many(&mut batch, 8),
    Ok(5),
    "the choice lock reads through"
  );
}

/// X3, O4: three lanes kept full, `PassLimit(f)` on each: between two items of a lane, at most
/// `f + n - 1` items of other lanes.
#[test]
fn x3_o4_the_count_bounds_a_lanes_wait() {
  let f = 3;
  let (tx, rx) =
    channel_with::<u32, Three, _>(three(4, false), PassLimit::new(NonZeroU32::new(f).unwrap()))
      .unwrap();
  let mut served = Vec::new();
  for round in 0..90 {
    for lane in Three::ALL {
      while tx.try_send(lane, round).is_ok() {}
    }
    served.push(rx.try_recv().unwrap().unwrap().lane());
  }
  for lane in Three::ALL {
    let at: Vec<usize> = served
      .iter()
      .enumerate()
      .filter(|(_, each)| **each == lane)
      .map(|(index, _)| index)
      .collect();
    assert!(at.len() > 1, "{lane:?} starved");
    for pair in at.windows(2) {
      assert!(pair[1] - pair[0] - 1 <= f as usize + 2, "{lane:?}: {at:?}");
    }
  }
}

/// X3, O5: with `Elapsed`, a lane whose oldest item has waited longer than its wait is served at
/// the next receive, over a lane of higher priority that is always full.
#[cfg(feature = "test-util")]
#[test]
fn x3_o5_the_time_bounds_a_lanes_wait() {
  use core::time::Duration;

  use crate::{Elapsed, ManualClock};

  let clock = ManualClock::new();
  let policy = Elapsed::with_clock(Duration::from_millis(10), clock.clone());
  let (tx, rx) = channel_with::<u32, Three, _>(three(4, false), policy).unwrap();
  tx.try_send(Three::C, 99).unwrap();
  tx.try_send(Three::A, 1).unwrap();
  assert_eq!(
    rx.try_recv().unwrap().unwrap().lane(),
    Three::A,
    "nothing is overdue: priority"
  );
  clock.advance(Duration::from_millis(11));
  while tx.try_send(Three::A, 7).is_ok() {}
  assert_eq!(rx.try_recv().unwrap().unwrap().lane(), Three::C, "overdue");
}
