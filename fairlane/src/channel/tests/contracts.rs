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

/// What a re-entrant policy does inside `owed`.
#[derive(Debug, Clone, Copy)]
enum Call {
  Nothing,
  Receive,
  WaitForRoom,
  PollQueued,
}

/// A policy that calls back into its own channel from `owed`, through handles it is given.
struct Reentrant {
  call: std::sync::Mutex<Call>,
  rx: std::sync::Mutex<Option<Receiver<u32, Three, Reentrant>>>,
  tx: std::sync::Mutex<Option<Sender<u32, Three, Reentrant>>>,
  /// A reserve already in a line, for `PollQueued`.
  queued: std::sync::Mutex<Option<crate::ReserveOwned<u32, Three, Reentrant>>>,
}

impl Policy<Three> for Reentrant {
  type Stamp = ();

  fn stamp(&self) {}

  fn owed(&self, _: &LaneView<Three, ()>) -> Option<Three> {
    let call = *self.call.lock().unwrap();
    match call {
      Call::Nothing => {}
      Call::Receive => {
        let rx = self.rx.lock().unwrap().clone().unwrap();
        let _ = rx.try_recv();
      }
      Call::WaitForRoom => {
        let tx = self.tx.lock().unwrap().clone().unwrap();
        let _ = tx.send_blocking(Three::C, 9);
      }
      Call::PollQueued => {
        let mut reserve = self.queued.lock().unwrap().take().unwrap();
        let _ = poll_with(&mut reserve, core::task::Waker::noop());
        *self.queued.lock().unwrap() = Some(reserve);
      }
    }
    None
  }
}

/// A channel whose policy holds its own handles, with lanes B and C full: two lanes in view, so
/// every receive asks the policy.
fn reentrant(
  call: Call,
) -> (
  Sender<u32, Three, Reentrant>,
  Receiver<u32, Three, Reentrant>,
) {
  let options = Options::from_fn(|_| LaneOptions::new(cap(1)));
  let policy = Reentrant {
    call: std::sync::Mutex::new(Call::Nothing),
    rx: std::sync::Mutex::new(None),
    tx: std::sync::Mutex::new(None),
    queued: std::sync::Mutex::new(None),
  };
  let (tx, rx) = channel_with(options, policy).unwrap();
  *rx.policy().rx.lock().unwrap() = Some(rx.clone());
  *tx.policy().tx.lock().unwrap() = Some(tx.clone());
  tx.try_send(Three::B, 1).unwrap();
  tx.try_send(Three::C, 2).unwrap();
  *tx.policy().call.lock().unwrap() = call;
  (tx, rx)
}

/// The policy's own handles go, so that the channel is not kept alive by itself.
fn release(tx: &Sender<u32, Three, Reentrant>) {
  tx.policy().rx.lock().unwrap().take();
  tx.policy().tx.lock().unwrap().take();
  tx.policy().queued.lock().unwrap().take();
}

fn message(payload: Box<dyn core::any::Any + Send>) -> String {
  payload
    .downcast::<String>()
    .map(|message| *message)
    .unwrap_or_default()
}

/// A policy that receives from its own channel inside a choice is refused by name. The receive
/// unwinds with nothing lost, and the same thread receives again afterwards: the marker went with
/// the unwind.
#[test]
fn a_policy_that_receives_from_its_channel_is_refused() {
  let (tx, rx) = reentrant(Call::Receive);
  let refused = catch_unwind(AssertUnwindSafe(|| rx.try_recv())).unwrap_err();
  assert!(message(refused).contains("received on the channel it serves"));
  *tx.policy().call.lock().unwrap() = Call::Nothing;
  let snapshot = tx.snapshot();
  assert_eq!(
    (snapshot.queued(Three::B), snapshot.queued(Three::C)),
    (1, 1)
  );
  assert_eq!(rx.try_recv().unwrap().unwrap().lane(), Three::B);
  release(&tx);
}

/// A policy that waits for room in its own channel, inside a choice, is refused by name instead of
/// parking for ever, and the receive unwinds with no room lost.
#[test]
fn a_policy_that_waits_for_room_in_its_channel_is_refused() {
  let (tx, rx) = reentrant(Call::WaitForRoom);
  let refused = catch_unwind(AssertUnwindSafe(|| rx.try_recv())).unwrap_err();
  assert!(message(refused).contains("waited for room on the channel it serves"));
  *tx.policy().call.lock().unwrap() = Call::Nothing;
  let snapshot = tx.snapshot();
  assert_eq!(
    (
      snapshot.queued(Three::C),
      snapshot.remaining(Three::C),
      snapshot.senders_waiting(Three::C)
    ),
    (1, 0, 0)
  );
  assert_eq!(rx.try_recv().unwrap().unwrap().lane(), Three::B);
  release(&tx);
}

/// A policy that polls, inside a choice, a reserve already in its own channel's line is refused by
/// name before the reserve waits again, as a reserve that would join the line is.
#[test]
fn a_policy_that_drives_a_queued_reserve_is_refused() {
  let (tx, rx) = reentrant(Call::Nothing);
  let mut queued = tx.reserve_owned(Three::C);
  assert!(
    poll_with(&mut queued, core::task::Waker::noop()).is_pending(),
    "lane C is full"
  );
  *tx.policy().queued.lock().unwrap() = Some(queued);
  *tx.policy().call.lock().unwrap() = Call::PollQueued;
  let refused = catch_unwind(AssertUnwindSafe(|| rx.try_recv())).unwrap_err();
  assert!(message(refused).contains("waited for room on the channel it serves"));
  *tx.policy().call.lock().unwrap() = Call::Nothing;
  assert_eq!(tx.snapshot().queued(Three::C), 1);
  assert_eq!(rx.try_recv().unwrap().unwrap().lane(), Three::B);
  release(&tx);
}

/// A `stamp` that panics while a sender waits for the permit's room gives the room back, and grants
/// it to that sender, before the item is dropped: a destructor that joins the sender ends.
#[test]
fn a_stamp_that_panics_refunds_before_the_item_drops() {
  type Handle = StdArc<std::sync::Mutex<Option<std::thread::JoinHandle<()>>>>;

  /// Joins the thread in its slot when it is dropped.
  struct Joiner(Handle);

  impl Drop for Joiner {
    fn drop(&mut self) {
      if let Some(handle) = self.0.lock().unwrap().take() {
        handle.join().unwrap();
      }
    }
  }

  let options = Options::from_fn(|_| LaneOptions::new(cap(1)));
  let (tx, _rx) = channel_with::<Joiner, Three, Stamping>(options, Stamping::default()).unwrap();
  let permit = tx.try_reserve(Three::B).unwrap();
  let producer = {
    let tx = tx.clone();
    std::thread::spawn(move || drop(tx.reserve_blocking(Three::B).unwrap()))
  };
  while tx.snapshot().senders_waiting(Three::B) == 0 {
    std::thread::yield_now();
  }
  let slot: Handle = StdArc::default();
  *slot.lock().unwrap() = Some(producer);
  let item = Joiner(slot.clone());
  tx.policy().armed.store(true, Ordering::SeqCst);
  let sent = catch_unwind(AssertUnwindSafe(move || permit.send(item)));
  tx.policy().armed.store(false, Ordering::SeqCst);
  assert!(sent.is_err(), "the stamp panicked");
  assert!(slot.lock().unwrap().is_none(), "the item joined the sender");
  assert_eq!(tx.snapshot().senders_waiting(Three::B), 0);
}

/// A policy whose `owed` panics while armed.
#[derive(Default)]
struct Moody {
  armed: AtomicBool,
}

impl Policy<Three> for Moody {
  type Stamp = ();

  fn stamp(&self) {}

  fn owed(&self, _: &LaneView<Three, ()>) -> Option<Three> {
    if self.armed.load(Ordering::SeqCst) {
      panic!("a policy that panics");
    }
    None
  }
}

/// A woken receive whose take unwinds strands no other receiver: the notice it held passes to the
/// next receive in line that holds none. Two commits notify the first and second receives, one
/// notice each; the first's take unwinds, and its notice wakes the third. The second and third
/// then take the two items, and the receive that unwound joins the lines again.
#[test]
fn a_receive_whose_take_unwinds_passes_its_notifications_on() {
  let options = Options::from_fn(|_| LaneOptions::new(cap(1)));
  let (tx, rx) = channel_with::<u32, Three, Moody>(options, Moody::default()).unwrap();
  let (first_probe, first_waker) = Probe::new();
  let (second_probe, second_waker) = Probe::new();
  let (third_probe, third_waker) = Probe::new();
  let mut first = rx.recv();
  let mut second = rx.recv();
  let mut third = rx.recv();
  assert!(poll_with(&mut first, &first_waker).is_pending());
  assert!(poll_with(&mut second, &second_waker).is_pending());
  assert!(poll_with(&mut third, &third_waker).is_pending());
  tx.try_send(Three::B, 1).unwrap();
  tx.try_send(Three::C, 2).unwrap();
  assert!(
    first_probe.take() && second_probe.take(),
    "one notice per commit, to distinct receives"
  );
  assert!(!third_probe.take());

  rx.policy().armed.store(true, Ordering::SeqCst);
  let unwound = catch_unwind(AssertUnwindSafe(|| poll_with(&mut first, &first_waker)));
  rx.policy().armed.store(false, Ordering::SeqCst);
  assert!(unwound.is_err(), "the policy panicked inside the take");
  assert!(
    third_probe.take(),
    "the notice the first receive held passed to the third"
  );
  assert!(matches!(
    poll_with(&mut second, &second_waker),
    Poll::Ready(Ok(_))
  ));
  assert!(matches!(
    poll_with(&mut third, &third_waker),
    Poll::Ready(Ok(_))
  ));
  assert!(
    poll_with(&mut first, &first_waker).is_pending(),
    "the receive that unwound joins the lines again"
  );
}
