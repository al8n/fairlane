use core::{num::NonZeroUsize, task::Poll};
use std::sync::{
  Arc as StdArc,
  atomic::{AtomicUsize, Ordering::SeqCst},
};

use super::*;
use crate::{
  Closed, Ended, Quota, RecvManyError, SendError, TryReserveError, TrySendError,
  testing::{Probe, poll_with},
};

fn cap(n: usize) -> NonZeroUsize {
  NonZeroUsize::new(n).unwrap()
}

#[test]
fn one_lane_moves_items_in_order_with_their_ordinals() {
  let (tx, rx) = bounded::<u32>(cap(4)).unwrap();
  for item in 0..4 {
    tx.try_send(Single, item).unwrap();
  }
  assert_eq!(tx.try_send(Single, 9), Err(TrySendError::Full(9)));
  for expected in 0..4 {
    let received = rx.try_recv().unwrap().unwrap();
    assert_eq!(received.lane(), Single);
    assert_eq!(received.ordinal(), u64::from(expected));
    assert_eq!(*received.item(), expected);
  }
  assert!(rx.try_recv().unwrap().is_none());
  tx.send_blocking(Single, 5).unwrap();
  assert_eq!(rx.recv_blocking().unwrap().ordinal(), 4);
}

#[test]
fn try_send_is_full_while_a_reserve_waits() {
  let (tx, _rx) = bounded::<u32>(cap(1)).unwrap();
  let permit = tx.try_reserve(Single).unwrap();
  let (_, waker) = Probe::new();
  let mut waiting = tx.reserve(Single);
  assert!(poll_with(&mut waiting, &waker).is_pending());
  drop(permit);
  assert_eq!(tx.try_send(Single, 1), Err(TrySendError::Full(1)));
}

#[test]
fn try_recv_many_takes_up_to_the_limit() {
  let (tx, rx) = bounded::<u32>(cap(8)).unwrap();
  for item in 0..6 {
    tx.try_send(Single, item).unwrap();
  }
  let mut batch = Vec::new();
  assert_eq!(rx.try_recv_many(&mut batch, 4), Ok(4));
  assert_eq!(
    rx.try_recv_many(&mut batch, Quota::up_to(8).limit_with(Single, 1)),
    Ok(1)
  );
  assert_eq!(rx.try_recv_many(&mut batch, 8), Ok(1));
  assert_eq!(rx.try_recv_many(&mut batch, 8), Ok(0));
  let items: Vec<u32> = batch
    .into_iter()
    .map(|received| received.into_item())
    .collect();
  assert_eq!(items, [0, 1, 2, 3, 4, 5]);
}

#[test]
fn an_empty_quota_is_refused_by_name() {
  let (tx, rx) = bounded::<u32>(cap(2)).unwrap();
  tx.try_send(Single, 1).unwrap();
  let mut batch = Vec::new();
  for quota in [
    Quota::up_to(0),
    Quota::up_to(4).limit_with(Single, 0),
    Quota::up_to(4).lanes(..Single),
  ] {
    assert_eq!(
      rx.try_recv_many(&mut batch, &quota),
      Err(RecvManyError::EmptyQuota)
    );
    assert_eq!(
      rx.recv_many_blocking(&mut batch, &quota),
      Err(RecvManyError::EmptyQuota)
    );
    let (_, waker) = Probe::new();
    let mut future = rx.recv_many(&mut batch, &quota);
    assert_eq!(
      poll_with(&mut future, &waker),
      Poll::Ready(Err(RecvManyError::EmptyQuota))
    );
  }
  assert!(batch.is_empty());
  assert_eq!(tx.snapshot().queued(Single), 1);
}

#[test]
fn a_close_ends_the_receive_after_what_is_queued() {
  let (tx, rx) = bounded::<u32>(cap(2)).unwrap();
  tx.try_send(Single, 1).unwrap();
  assert!(!tx.is_closed());
  assert!(rx.close());
  assert!(!tx.close());
  assert!(tx.is_closed() && rx.is_closed());
  assert_eq!(tx.try_send(Single, 2), Err(TrySendError::Closed(2)));
  assert_eq!(tx.try_reserve(Single).unwrap_err(), TryReserveError::Closed);
  assert_eq!(tx.send_blocking(Single, 3), Err(SendError(3)));
  assert!(matches!(tx.reserve_blocking(Single), Err(Closed)));
  assert_eq!(rx.try_recv().unwrap().unwrap().into_item(), 1);
  assert!(matches!(rx.try_recv(), Err(Ended)));
  assert!(matches!(rx.recv_blocking(), Err(Ended)));
  let mut batch = Vec::new();
  assert_eq!(rx.try_recv_many(&mut batch, 4), Err(RecvManyError::Ended));
  assert_eq!(
    rx.recv_many_blocking(&mut batch, 4),
    Err(RecvManyError::Ended)
  );
}

#[test]
fn the_last_sender_closes_and_the_queue_drains() {
  let (tx, rx) = bounded::<u32>(cap(2)).unwrap();
  let other = tx.clone();
  tx.try_send(Single, 1).unwrap();
  drop(tx);
  assert!(!other.is_closed());
  drop(other);
  assert!(rx.is_closed());
  assert_eq!(rx.recv_blocking().unwrap().into_item(), 1);
  assert!(matches!(rx.recv_blocking(), Err(Ended)));
}

#[test]
fn an_owned_permit_keeps_the_channel_open() {
  let (tx, rx) = bounded::<u32>(cap(2)).unwrap();
  let (_, waker) = Probe::new();
  let mut reserve = tx.reserve_owned(Single);
  let Poll::Ready(Ok(permit)) = poll_with(&mut reserve, &waker) else {
    panic!("room is free");
  };
  drop(reserve);
  drop(tx);
  assert!(!rx.is_closed(), "the owned permit holds a sender");
  assert_eq!(permit.lane(), Single);
  permit.send(7).unwrap();
  assert!(rx.is_closed(), "its sender went with the send");
  assert_eq!(rx.recv_blocking().unwrap().into_item(), 7);
}

#[test]
fn a_dropped_owned_permit_gives_its_room_back() {
  let (tx, _rx) = bounded::<u32>(cap(1)).unwrap();
  let (_, waker) = Probe::new();
  let mut reserve = tx.reserve_owned(Single);
  let Poll::Ready(Ok(permit)) = poll_with(&mut reserve, &waker) else {
    panic!("room is free");
  };
  assert_eq!(tx.snapshot().remaining(Single), 0);
  drop(permit);
  assert_eq!(tx.snapshot().remaining(Single), 1);
}

#[test]
fn when_closed_resolves_at_the_close() {
  let (tx, rx) = bounded::<u32>(cap(1)).unwrap();
  let (probe, waker) = Probe::new();
  let mut closed = tx.closed();
  assert!(poll_with(&mut closed, &waker).is_pending());
  drop(rx);
  assert!(probe.take());
  assert_eq!(poll_with(&mut closed, &waker), Poll::Ready(()));
}

/// Counts its drops; panics in its drop where told.
struct Counted(StdArc<AtomicUsize>, bool);

impl Drop for Counted {
  fn drop(&mut self) {
    self.0.fetch_add(1, SeqCst);
    if self.1 {
      panic!("a destructor that panics");
    }
  }
}

#[test]
fn the_last_receiver_disconnects() {
  let drops = StdArc::new(AtomicUsize::new(0));
  let (tx, rx) = bounded::<Counted>(cap(2)).unwrap();
  tx.try_send(Single, Counted(drops.clone(), false)).unwrap();
  let permit = tx.try_reserve(Single).unwrap();
  drop(rx);
  assert_eq!(drops.load(SeqCst), 1, "the queued item is let go");
  let SendError(item) = permit.send(Counted(drops.clone(), false)).unwrap_err();
  drop(item);
  assert_eq!(drops.load(SeqCst), 2);
  assert!(matches!(tx.reserve_blocking(Single), Err(Closed)));
  let snapshot = tx.snapshot();
  assert_eq!(
    (snapshot.remaining(Single), snapshot.queued(Single)),
    (2, 0)
  );
  assert_eq!(snapshot.received(Single), 1);
}

#[test]
fn disconnect_drops_every_item_under_its_own_catch() {
  let drops = StdArc::new(AtomicUsize::new(0));
  let (tx, rx) = bounded::<Counted>(cap(4)).unwrap();
  for panics in [false, true, false, true] {
    tx.try_send(Single, Counted(drops.clone(), panics)).unwrap();
  }
  let hook = std::panic::take_hook();
  std::panic::set_hook(Box::new(|_| {}));
  drop(rx);
  std::panic::set_hook(hook);
  assert_eq!(drops.load(SeqCst), 4);
}

/// A payload whose own drop panics.
struct Bomb;

impl Drop for Bomb {
  fn drop(&mut self) {
    panic!("a panic payload whose drop panics");
  }
}

/// Counts its drops, and panics in its drop with a payload whose drop panics too.
struct Volatile(StdArc<AtomicUsize>);

impl Drop for Volatile {
  fn drop(&mut self) {
    self.0.fetch_add(1, SeqCst);
    std::panic::panic_any(Bomb);
  }
}

/// Both drains of the last receiver's drop, the lookahead's and the rings', drop every item, though
/// each destructor panics with a payload whose own drop panics.
#[test]
fn disconnect_survives_a_payload_whose_drop_panics() {
  use crate::testing::Three;

  let drops = StdArc::new(AtomicUsize::new(0));
  let options = Options::from_fn(|_| LaneOptions::new(cap(2)));
  let (tx, rx) = channel::<Volatile, Three>(options).unwrap();
  tx.try_send(Three::B, Volatile(drops.clone())).unwrap();
  tx.try_send(Three::C, Volatile(drops.clone())).unwrap();
  tx.try_send(Three::C, Volatile(drops.clone())).unwrap();
  let taken = rx.try_recv().unwrap().unwrap();
  assert_eq!(
    taken.lane(),
    Three::B,
    "and C's oldest item is in the lookahead"
  );
  let dropped = std::panic::catch_unwind(core::panic::AssertUnwindSafe(move || drop(taken)));
  crate::wake::dispose(dropped.unwrap_err());
  drops.store(0, SeqCst);
  drop(rx);
  assert_eq!(drops.load(SeqCst), 2, "both items left were dropped");
  assert_eq!(tx.snapshot().received(Three::C), 2);
}

/// A sink with room for a set number of items: a batch's `Vec` that cannot grow.
struct Cramped<L: Lane> {
  items: Vec<crate::Received<u32, L>>,
  room: usize,
}

impl<L: Lane> crate::receiver::Sink<u32, L> for Cramped<L> {
  fn room(&mut self, _: bool) -> bool {
    self.items.len() < self.room
  }

  fn put(&mut self, received: crate::Received<u32, L>) {
    self.items.push(received);
  }
}

/// A batch whose sink has no room left stops before it takes another item: every item it took is
/// the sink's, the rest stay queued, and every room it took is given back, to a waiting reserve
/// first.
#[test]
fn a_batch_stops_where_its_sink_has_no_room() {
  use crate::{receiver::Took, testing::Three};

  let (tx, _rx) = bounded::<u32>(cap(2)).unwrap();
  tx.try_send(Single, 1).unwrap();
  tx.try_send(Single, 2).unwrap();
  let (probe, waker) = Probe::new();
  let mut waiting = tx.reserve(Single);
  assert!(poll_with(&mut waiting, &waker).is_pending());
  let mut sink = Cramped {
    items: Vec::new(),
    room: 1,
  };
  assert!(matches!(
    tx.shared().take(&Quota::up_to(8), &mut sink),
    Took::Items(held) if held.get() == 1
  ));
  assert_eq!(sink.items.len(), 1);
  assert_eq!(
    tx.snapshot().queued(Single),
    1,
    "the second item stays queued"
  );
  assert!(
    probe.take(),
    "the taken item's room went to the waiting reserve"
  );

  let options = Options::from_fn(|_| LaneOptions::new(cap(4)));
  let (tx, _rx) = channel::<u32, Three>(options).unwrap();
  for item in 0..3 {
    tx.try_send(Three::B, item).unwrap();
  }
  let mut sink = Cramped {
    items: Vec::new(),
    room: 2,
  };
  assert!(matches!(
    tx.shared().take(&Quota::up_to(8), &mut sink),
    Took::Items(held) if held.get() == 2
  ));
  let snapshot = tx.snapshot();
  assert_eq!(
    (snapshot.queued(Three::B), snapshot.remaining(Three::B)),
    (1, 3)
  );
}

/// The last receiver's drop wakes the reserves its close resolves before it drops any item: an
/// item whose destructor joins a producer parked on the channel lets that producer return.
#[test]
fn a_disconnect_wakes_its_waiters_before_dropping_items() {
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

  let slot: Handle = StdArc::default();
  let (tx, rx) = bounded::<Joiner>(cap(1)).unwrap();
  tx.try_send(Single, Joiner(slot.clone())).unwrap();
  let producer = {
    let tx = tx.clone();
    std::thread::spawn(move || {
      assert!(matches!(tx.reserve_blocking(Single), Err(Closed)));
    })
  };
  while tx.snapshot().senders_waiting(Single) == 0 {
    std::thread::yield_now();
  }
  *slot.lock().unwrap() = Some(producer);
  drop(rx);
  assert!(
    slot.lock().unwrap().is_none(),
    "the item joined the producer"
  );
}

type OtherPermit = crate::OwnedPermit<u32, Single, DefaultPolicy<Single>>;

/// A panic payload that panics, at its drop, with the next link of a chain, and the last link with
/// the permit it carries.
struct HandsOver(usize, Option<OtherPermit>);

impl Drop for HandsOver {
  fn drop(&mut self) {
    let permit = self.1.take();
    match self.0 {
      0 => std::panic::panic_any(permit),
      links => std::panic::panic_any(HandsOver(links - 1, permit)),
    }
  }
}

/// An item whose drop panics with the first link of a long chain of `HandsOver` payloads.
struct Passes(Option<OtherPermit>);

impl Drop for Passes {
  fn drop(&mut self) {
    std::panic::panic_any(HandsOver(16, self.0.take()));
  }
}

/// A drained item's panic payload whose drop panics with a payload of its own, and so on along a
/// long chain, is disposed of link by link to the end: the last payload, another channel's permit,
/// is dropped, and its room is given back.
#[test]
fn a_disposal_drops_the_payload_its_payload_hands_on() {
  let (other, other_rx) = bounded::<u32>(cap(1)).unwrap();
  let mut reserve = other.reserve_owned(Single);
  let Poll::Ready(Ok(permit)) = poll_with(&mut reserve, core::task::Waker::noop()) else {
    panic!("the other channel has room");
  };
  let (tx, rx) = bounded::<Passes>(cap(1)).unwrap();
  tx.try_send(Single, Passes(Some(permit))).unwrap();
  assert_eq!(other.snapshot().remaining(Single), 0);
  drop(rx);
  assert_eq!(
    other.snapshot().remaining(Single),
    1,
    "the permit's room is back"
  );
  drop(other_rx);
}

/// A batch that grants the room of every item it takes to a waiting reserve wakes every one of
/// them, past the few wakers it keeps inline: each waker's place is made before its item is taken.
#[test]
fn a_batch_wakes_every_reserve_it_grants() {
  use crate::testing::Three;

  const ITEMS: usize = 12;

  let (tx, rx) = bounded::<u32>(cap(ITEMS)).unwrap();
  for item in 0..ITEMS {
    tx.try_send(Single, item as u32).unwrap();
  }
  let mut waiting: Vec<_> = (0..ITEMS)
    .map(|_| {
      let (probe, waker) = Probe::new();
      let mut reserve = tx.reserve(Single);
      assert!(poll_with(&mut reserve, &waker).is_pending());
      (probe, waker, reserve)
    })
    .collect();
  let mut batch = Vec::new();
  assert_eq!(rx.try_recv_many(&mut batch, ITEMS), Ok(ITEMS));
  for (probe, waker, reserve) in &mut waiting {
    assert!(probe.take(), "every granted reserve is woken");
    assert!(matches!(poll_with(reserve, waker), Poll::Ready(Ok(_))));
  }

  let options = Options::from_fn(|_| LaneOptions::new(cap(ITEMS / 2)));
  let (tx, rx) = channel::<u32, Three>(options).unwrap();
  for item in 0..ITEMS / 2 {
    tx.try_send(Three::B, item as u32).unwrap();
    tx.try_send(Three::C, item as u32).unwrap();
  }
  let mut waiting: Vec<_> = [Three::B, Three::C]
    .into_iter()
    .flat_map(|lane| (0..ITEMS / 2).map(move |_| lane))
    .map(|lane| {
      let (probe, waker) = Probe::new();
      let mut reserve = tx.reserve(lane);
      assert!(poll_with(&mut reserve, &waker).is_pending());
      (probe, waker, reserve)
    })
    .collect();
  let mut batch = Vec::new();
  assert_eq!(rx.try_recv_many(&mut batch, ITEMS), Ok(ITEMS));
  for (probe, waker, reserve) in &mut waiting {
    assert!(probe.take(), "every granted reserve is woken");
    assert!(matches!(poll_with(reserve, waker), Poll::Ready(Ok(_))));
  }
}

/// A sink that can make no room at all: it panics when asked, as a `Vec` at its limit does.
struct NoRoom;

impl<L: Lane> crate::receiver::Sink<u32, L> for NoRoom {
  fn room(&mut self, _: bool) -> bool {
    panic!("no room");
  }

  fn put(&mut self, _: crate::Received<u32, L>) {
    unreachable!("no room was made");
  }
}

/// A receive of an empty or ended lane asks its sink for no room: it answers empty, or ended.
#[test]
fn an_empty_lane_asks_its_sink_for_no_room() {
  use crate::{receiver::Took, testing::Three};

  let (tx, _rx) = bounded::<u32>(cap(1)).unwrap();
  assert!(matches!(
    tx.shared().take(&Quota::up_to(4), &mut NoRoom),
    Took::Nothing
  ));
  tx.close();
  assert!(matches!(
    tx.shared().take(&Quota::up_to(4), &mut NoRoom),
    Took::Ended
  ));

  let (tx, _rx) = channel::<u32, Three>(Options::from_fn(|_| LaneOptions::new(cap(1)))).unwrap();
  assert!(matches!(
    tx.shared().take(&Quota::up_to(4), &mut NoRoom),
    Took::Nothing
  ));
}

#[test]
fn snapshot_reads_each_lane() {
  let (tx, rx) = bounded::<u32>(cap(4)).unwrap();
  tx.try_send(Single, 1).unwrap();
  tx.try_send(Single, 2).unwrap();
  let permit = tx.try_reserve(Single).unwrap();
  let snapshot = rx.snapshot();
  assert_eq!(snapshot.queued(Single), 2);
  assert_eq!(snapshot.reserved(Single), 1);
  assert_eq!(snapshot.remaining(Single), 1);
  assert_eq!(snapshot.committed(Single), 2);
  assert_eq!(snapshot.received(Single), 0);
  assert_eq!(snapshot.senders_waiting(Single), 0);
  assert_eq!(snapshot.receivers_waiting(Single), 0);
  assert!(!snapshot.is_closed());
  drop(permit);
  rx.try_recv().unwrap();
  assert_eq!(rx.snapshot().received(Single), 1);
}

#[test]
fn a_waiting_receive_counts_on_its_lane_until_it_leaves() {
  let (tx, rx) = bounded::<u32>(cap(2)).unwrap();
  let (probe, waker) = Probe::new();
  let mut recv = rx.recv();
  assert!(poll_with(&mut recv, &waker).is_pending());
  assert_eq!(rx.snapshot().receivers_waiting(Single), 1);
  tx.try_send(Single, 3).unwrap();
  assert!(probe.take());
  let Poll::Ready(Ok(received)) = poll_with(&mut recv, &waker) else {
    panic!("the item is there");
  };
  assert_eq!(received.into_item(), 3);
  assert_eq!(rx.snapshot().receivers_waiting(Single), 0);
  let mut recv = rx.recv();
  assert!(poll_with(&mut recv, &waker).is_pending());
  drop(recv);
  assert_eq!(rx.snapshot().receivers_waiting(Single), 0);
}

#[test]
fn a_capacity_that_cannot_be_held_is_refused() {
  assert_eq!(
    bounded::<u8>(NonZeroUsize::MAX).unwrap_err(),
    Malformed::Capacity(0)
  );
  assert_eq!(
    bounded::<[u8; 1 << 20]>(cap(usize::MAX >> 4)).unwrap_err(),
    Malformed::Capacity(0)
  );
}

/// The shared state holds no item and no lane state inline, whatever the lane count: a channel of
/// large items, or of many lanes, is built without moving them through the stack.
#[test]
fn shared_state_holds_no_item_inline() {
  type Large = [u8; 1 << 16];
  assert!(size_of::<Shared<Large, Single, DefaultPolicy<Single>>>() < size_of::<Large>());
  assert!(
    size_of::<Shared<Large, crate::testing::Three, DefaultPolicy<crate::testing::Three>>>()
      < size_of::<Large>()
  );
  assert!(
    size_of::<Shared<u8, crate::testing::Three, crate::Strict>>() < size_of::<LaneState<u8, ()>>(),
    "three lanes' states would not fit in less than one"
  );
}

mod contracts;
mod differential;
#[cfg(feature = "test-util")]
mod differential_lanes;
mod lanes;
mod linearizability;
mod stress;

/// Each commit notifies the next receiver in line, even while an earlier notice is outstanding: a
/// notified receive whose task is busy elsewhere holds back no other receiver.
#[test]
fn each_commit_notifies_the_next_receiver_while_one_is_outstanding() {
  let (tx, rx) = bounded::<u32>(cap(2)).unwrap();
  let (first_probe, first_waker) = Probe::new();
  let (second_probe, second_waker) = Probe::new();
  let mut first = rx.recv();
  let mut second = rx.recv();
  assert!(poll_with(&mut first, &first_waker).is_pending());
  assert!(poll_with(&mut second, &second_waker).is_pending());
  tx.try_send(Single, 1).unwrap();
  assert!(
    first_probe.take(),
    "the first commit notifies the oldest receiver"
  );
  assert!(!second_probe.take());
  // The first receive's task is busy elsewhere, and does not poll it yet.
  tx.try_send(Single, 2).unwrap();
  assert!(
    second_probe.take(),
    "the second commit notifies the next receiver"
  );
  assert!(matches!(
    poll_with(&mut second, &second_waker),
    Poll::Ready(Ok(_))
  ));
  assert!(matches!(
    poll_with(&mut first, &first_waker),
    Poll::Ready(Ok(_))
  ));
}

/// A notified receive that is cancelled passes its notice on to the next receiver in line, even
/// while another notice is outstanding.
#[test]
fn a_cancelled_notified_receive_passes_its_notice_on_while_another_is_outstanding() {
  let (tx, rx) = bounded::<u32>(cap(2)).unwrap();
  let probes: Vec<_> = (0..3).map(|_| Probe::new()).collect();
  let mut receives: Vec<_> = (0..3).map(|_| rx.recv()).collect();
  for (receive, (_, waker)) in receives.iter_mut().zip(&probes) {
    assert!(poll_with(receive, waker).is_pending());
  }
  tx.try_send(Single, 1).unwrap();
  tx.try_send(Single, 2).unwrap();
  assert!(
    probes[0].0.take() && probes[1].0.take(),
    "one notice per commit"
  );
  assert!(!probes[2].0.take());
  drop(receives.remove(0));
  assert!(
    probes[2].0.take(),
    "the cancelled notice passes to the third receiver"
  );
  for (receive, (_, waker)) in receives.iter_mut().zip(&probes[1..]) {
    assert!(matches!(poll_with(receive, waker), Poll::Ready(Ok(_))));
  }
}
