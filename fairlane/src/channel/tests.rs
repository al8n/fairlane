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

mod differential;
#[cfg(feature = "test-util")]
mod differential_lanes;
mod lanes;
mod linearizability;
mod stress;
