//! Laws of a channel of several lanes.

use core::{
  num::{NonZeroU32, NonZeroUsize},
  panic::AssertUnwindSafe,
  sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::panic::catch_unwind;

use super::*;
use crate::{
  LaneView, PassLimit, Policy, Quota, Received, Strict,
  lane::Lane,
  testing::{Faults, Three},
};

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

/// A batch stops at a lane's limit and never passes the lane over.
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

/// A policy that panics inside a batch loses no item and leaks no room.
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

/// Three lanes kept full, `PassLimit(f)` on each: between two items of a lane, at most
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

/// With `Elapsed`, a lane whose oldest item has waited longer than its wait is served at
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

/// A receive asleep on every lane, dropped with a waker whose drop panics: it leaves every lane,
/// so the next commit to any of them wakes the receive behind it.
#[test]
fn a_dropped_receive_leaves_every_lane_though_its_waker_panics() {
  let (tx, rx) = channel_with::<u32, Three, _>(three(4, false), Strict).unwrap();
  let faults = StdArc::new(Faults::default());
  let faulty = faults.waker();
  let mut first = rx.recv();
  assert!(poll_with(&mut first, &faulty).is_pending());
  let (probe, waker) = Probe::new();
  let mut second = rx.recv();
  assert!(poll_with(&mut second, &waker).is_pending());

  faults.drop_panics.store(true, SeqCst);
  let dropped = catch_unwind(AssertUnwindSafe(move || drop(first)));
  faults.drop_panics.store(false, SeqCst);
  assert!(dropped.is_err(), "the receive's stored wakers were dropped");
  for lane in Three::ALL {
    assert_eq!(
      rx.snapshot().receivers_waiting(lane),
      1,
      "only the second receive waits on {lane:?}"
    );
  }
  tx.try_send(Three::C, 7).unwrap();
  assert!(
    probe.take(),
    "the commit woke the receive behind the dropped one"
  );
  assert!(matches!(poll_with(&mut second, &waker), Poll::Ready(Ok(_))));
  drop(faulty);
}

/// Panics on `owed` while armed.
struct PanicsWhileArmed(AtomicBool);

impl Policy<Three> for PanicsWhileArmed {
  type Stamp = ();

  fn stamp(&self) {}

  fn owed(&self, _: &LaneView<Three, ()>) -> Option<Three> {
    if self.0.load(Ordering::SeqCst) {
      panic!("a policy that panics");
    }
    None
  }
}

/// A receive that is notified, spends its notifications, and then unwinds out of its take still
/// wakes the receive behind it for the items it left.
///
/// The notifications land within one poll: the receive's take finds nothing, then it refreshes its
/// waker, and the new waker's clone commits two items. The clone runs with no lock held.
#[test]
fn a_woken_receive_whose_take_unwinds_wakes_the_next() {
  let policy = PanicsWhileArmed(AtomicBool::new(false));
  let (tx, rx) = channel_with::<u32, Three, _>(three(4, false), policy).unwrap();
  let (_, waker1) = Probe::new();
  let (probe2, waker2) = Probe::new();
  let mut first = rx.recv();
  let mut second = rx.recv();
  assert!(poll_with(&mut first, &waker1).is_pending());
  assert!(poll_with(&mut second, &waker2).is_pending());

  let faults = StdArc::new(Faults::default());
  let sender = tx.clone();
  *faults.on_clone.lock().unwrap() = Some(Box::new(move || {
    sender.try_send(Three::B, 1).unwrap();
    sender.try_send(Three::C, 2).unwrap();
  }));
  let faulty = faults.waker();
  rx.policy().0.store(true, Ordering::SeqCst);
  let unwound = catch_unwind(AssertUnwindSafe(|| poll_with(&mut first, &faulty)));
  rx.policy().0.store(false, Ordering::SeqCst);
  assert!(
    unwound.is_err(),
    "two lanes were in view, and the policy panicked"
  );
  assert!(
    probe2.take(),
    "the unwinding take woke the receive behind it"
  );
  assert!(matches!(
    poll_with(&mut second, &waker2),
    Poll::Ready(Ok(_))
  ));
}

/// A receive of one item makes no policy call after its take: a policy that panics on its second
/// call never costs the item its first call chose, and the panic of a later receive loses nothing.
#[test]
fn a_single_receive_asks_the_policy_once() {
  let policy = PanicsOnSecond(AtomicUsize::new(0));
  let (tx, rx) = channel_with::<u32, Three, _>(three(4, false), policy).unwrap();
  for item in 0..2 {
    tx.try_send(Three::B, item).unwrap();
    tx.try_send(Three::C, 10 + item).unwrap();
  }
  let first = rx.try_recv().unwrap().expect("items are queued");
  assert_eq!((first.lane(), first.into_item()), (Three::B, 0));
  assert_eq!(
    rx.policy().0.load(Ordering::SeqCst),
    1,
    "one call for one item"
  );
  let unwound = catch_unwind(AssertUnwindSafe(|| rx.try_recv()));
  assert!(unwound.is_err(), "the second receive's call panicked");
  let snapshot = tx.snapshot();
  assert_eq!(
    snapshot.queued(Three::B) + snapshot.queued(Three::C),
    3,
    "the panic took no item"
  );
}
