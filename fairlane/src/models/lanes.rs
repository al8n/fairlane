//! The waits and the choice of a channel of several lanes.

use core::{future::Future, num::NonZeroUsize};

use loom::{
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering::SeqCst},
  },
  thread,
};

use super::model;
use crate::{LaneOptions, Options, Quota, Receiver, Sender, Strict, channel_with, testing::Three};

/// Three lanes of capacity 1; lane A express where `express`.
fn lanes(express: bool) -> (Sender<u8, Three, Strict>, Receiver<u8, Three, Strict>) {
  let options = Options::from_fn(|lane| {
    let options = LaneOptions::new(NonZeroUsize::MIN);
    if lane == Three::A {
      options.with_express(express)
    } else {
      options
    }
  });
  channel_with(options, Strict).unwrap()
}

/// Lane B is full and a sender waits on it; a reserve on lane A completes with no receive.
#[test]
fn mp6_a_full_lane_never_holds_another_lanes_sender() {
  model(|| {
    let (tx, rx) = lanes(false);
    tx.try_send(Three::B, 0).unwrap();
    let parked = {
      let tx = tx.clone();
      thread::spawn(move || tx.reserve_blocking(Three::B).is_err())
    };
    let other = thread::spawn(move || {
      drop(tx.reserve_blocking(Three::A).unwrap());
      tx
    });
    let tx = other.join().unwrap();
    drop(rx);
    assert!(
      parked.join().unwrap(),
      "the parked sender resolves Closed at the disconnect"
    );
    drop(tx);
  });
}

/// Receiver A admits lane A only, receiver B every lane; a lane-B item is committed: B is woken
/// for it.
#[test]
fn ma2_a_commit_wakes_a_receiver_that_admits_its_lane() {
  model(|| {
    let (tx, rx) = lanes(false);
    let every = rx.clone();
    let only_a = thread::spawn(move || {
      let mut batch = Vec::new();
      let ended = rx
        .recv_many_blocking(&mut batch, Quota::up_to(1).lanes(..=Three::A))
        .is_err();
      (ended, rx)
    });
    let receiver = thread::spawn(move || every.recv_blocking().unwrap().lane());
    let sender = thread::spawn(move || {
      tx.try_send(Three::B, 1).unwrap();
      tx
    });
    assert_eq!(receiver.join().unwrap(), Three::B);
    let tx = sender.join().unwrap();
    tx.close();
    let (ended, rx) = only_a.join().unwrap();
    assert!(ended);
    drop((tx, rx));
  });
}

/// A receiver in the lines of lanes A and B, notified on both, takes one item and spends its other
/// notification; the receiver of lane B alone is woken by the chain for the other item.
#[test]
fn ma5_a_discarded_wake_is_repaired_by_the_chain() {
  model(|| {
    let (tx, rx) = lanes(false);
    let only_b = rx.clone();
    let both = thread::spawn(move || {
      let mut batch = Vec::new();
      rx.recv_many_blocking(&mut batch, Quota::up_to(1).lanes(..=Three::B))
        .unwrap();
      (batch.len(), rx)
    });
    let b = thread::spawn(move || {
      let mut batch = Vec::new();
      only_b
        .recv_many_blocking(&mut batch, Quota::up_to(1).lanes(Three::B..=Three::B))
        .unwrap();
      (batch.len(), only_b)
    });
    let sender = thread::spawn(move || {
      tx.try_send(Three::A, 1).unwrap();
      tx.try_send(Three::B, 2).unwrap();
      tx
    });
    let (one, rx) = both.join().unwrap();
    let (other, only_b) = b.join().unwrap();
    assert_eq!(one + other, 2);
    drop((sender.join().unwrap(), rx, only_b));
  });
}

/// A receive in the line of every lane, dropped while it may hold a notification on lanes A and
/// B: it passes each on, lane by lane, so the receiver of lane B alone takes B's item.
#[test]
fn ma7_a_dropped_receive_passes_on_each_lanes_notification() {
  model(|| {
    let (tx, rx) = lanes(false);
    let only_b = rx.clone();
    let listening = Arc::new(AtomicBool::new(false));
    let dropper = {
      let listening = listening.clone();
      thread::spawn(move || {
        let mut recv = rx.recv();
        let polled = core::pin::Pin::new(&mut recv).poll(&mut core::task::Context::from_waker(
          core::task::Waker::noop(),
        ));
        assert!(polled.is_pending());
        listening.store(true, SeqCst);
        drop(recv);
        rx
      })
    };
    let b = thread::spawn(move || {
      let mut batch = Vec::new();
      only_b
        .recv_many_blocking(&mut batch, Quota::up_to(1).lanes(Three::B..=Three::B))
        .unwrap();
      (batch[0].lane(), only_b)
    });
    let sender = thread::spawn(move || {
      while !listening.load(SeqCst) {
        thread::yield_now();
      }
      tx.try_send(Three::A, 1).unwrap();
      tx.try_send(Three::B, 2).unwrap();
      tx
    });
    let rx = dropper.join().unwrap();
    let (lane, only_b) = b.join().unwrap();
    assert_eq!(lane, Three::B);
    drop((sender.join().unwrap(), rx, only_b));
  });
}

/// One thread commits an express token and then an item; a receiver receives twice: the token
/// first.
#[test]
fn mt1_an_express_token_is_taken_before_a_later_item() {
  model(|| {
    let (tx, rx) = lanes(true);
    let sender = thread::spawn(move || {
      tx.try_send(Three::A, 0).unwrap();
      tx.try_send(Three::C, 1).unwrap();
      tx
    });
    let receiver = thread::spawn(move || {
      let first = rx.recv_blocking().unwrap().lane();
      let second = rx.recv_blocking().unwrap().lane();
      (first, second, rx)
    });
    let tx = sender.join().unwrap();
    let (first, second, rx) = receiver.join().unwrap();
    assert_eq!((first, second), (Three::A, Three::C));
    drop((tx, rx));
  });
}

/// Two receivers take batches of up to two over three queued items of two lanes: the batches are
/// disjoint, cover all three, and no choice of one falls inside the other's.
#[test]
fn mt2_two_batches_never_interleave() {
  model(|| {
    let options = Options::from_fn(|_| LaneOptions::new(NonZeroUsize::new(2).unwrap()));
    let (tx, rx) = channel_with::<u8, Three, _>(options, Strict).unwrap();
    tx.try_send(Three::A, 0).unwrap();
    tx.try_send(Three::A, 1).unwrap();
    tx.try_send(Three::B, 2).unwrap();
    let other = rx.clone();
    let take = |rx: Receiver<u8, Three, Strict>| {
      thread::spawn(move || {
        let mut batch = Vec::new();
        let _ = rx.try_recv_many(&mut batch, 2);
        (
          batch
            .into_iter()
            .map(|received| received.into_item())
            .collect::<Vec<u8>>(),
          rx,
        )
      })
    };
    let one = take(rx);
    let two = take(other);
    let (mut first, rx) = one.join().unwrap();
    let (second, other) = two.join().unwrap();
    assert!(
      first != [0, 2] && second != [0, 2],
      "a batch took A's first item and B's"
    );
    first.extend(second);
    first.sort_unstable();
    assert_eq!(
      first,
      [0, 1, 2],
      "the two batches cover the three items, once each"
    );
    drop((tx, rx, other));
  });
}

/// Sender A commits to lane A and returns; then sender B, which joins A first, commits to lane B;
/// a receiver beside them never returns B's item while A's is queued. B joins A rather than spin
/// on a flag: under loom, two threads that yield can be run in turn forever.
#[test]
fn mt3_a_later_commit_to_a_lower_lane_is_not_taken_first() {
  model(|| {
    let (tx, rx) = lanes(false);
    let a = {
      let tx = tx.clone();
      thread::spawn(move || {
        tx.try_send(Three::A, 1).unwrap();
      })
    };
    let b = thread::spawn(move || {
      a.join().unwrap();
      tx.try_send(Three::B, 2).unwrap();
      tx
    });
    let receiver = thread::spawn(move || {
      let first = rx.recv_blocking().unwrap().into_item();
      (first, rx)
    });
    let tx = b.join().unwrap();
    let (first, rx) = receiver.join().unwrap();
    assert_eq!(first, 1, "B's item was taken while A's was queued");
    drop((tx, rx));
  });
}

/// Linearizability under loom: every interleaving of two threads, two operations each, over three
/// lanes of capacity 1, is linearizable against the model.
#[test]
fn x2_small_histories_are_linearizable() {
  use loom::sync::atomic::AtomicU64;

  use crate::model::check::{Event, Op, linearizable, perform};

  model(|| {
    let options = Options::from_fn(|_| LaneOptions::new(NonZeroUsize::MIN));
    let (tx, rx) = channel_with::<u64, Three, _>(options.clone(), Strict).unwrap();
    let clock = Arc::new(AtomicU64::new(0));
    let shapes = [
      vec![
        Op::Send {
          lane: Three::B,
          item: 1,
        },
        Op::Recv {
          quota: Quota::up_to(2),
        },
      ],
      vec![
        Op::Send {
          lane: Three::A,
          item: 2,
        },
        Op::Recv {
          quota: Quota::up_to(1),
        },
      ],
    ];
    let handles: Vec<_> = shapes
      .into_iter()
      .map(|ops| {
        let (tx, rx, clock) = (tx.clone(), rx.clone(), clock.clone());
        thread::spawn(move || {
          ops
            .into_iter()
            .map(|op| {
              let call = clock.fetch_add(1, SeqCst);
              let answer = perform(&tx, &rx, &op);
              let ret = clock.fetch_add(1, SeqCst);
              Event {
                call,
                ret,
                op,
                answer,
              }
            })
            .collect::<Vec<_>>()
        })
      })
      .collect();
    let history: Vec<Event<Three>> = handles
      .into_iter()
      .flat_map(|handle| handle.join().unwrap())
      .collect();
    assert!(
      linearizable(&options, &Strict, &history),
      "no order of the model explains {history:#?}"
    );
    drop((tx, rx));
  });
}

/// A policy whose stamp is not zero-sized, so the receiving side moves each lane's oldest item
/// through its lookahead.
struct Stamped;

impl crate::Policy<Three> for Stamped {
  type Stamp = u8;

  fn stamp(&self) -> u8 {
    0
  }

  fn owed(&self, _: &crate::LaneView<Three, u8>) -> Option<Three> {
    None
  }
}

/// Snapshots beside receives that move items through the lookahead: `received` never goes
/// backward from one snapshot to the next.
#[test]
fn ms1_received_never_goes_backward() {
  model(|| {
    let options = Options::from_fn(|_| LaneOptions::new(NonZeroUsize::MIN));
    let (tx, rx) = channel_with::<u8, Three, Stamped>(options, Stamped).unwrap();
    tx.try_send(Three::B, 1).unwrap();
    tx.try_send(Three::C, 2).unwrap();
    let receiver = thread::spawn(move || {
      rx.try_recv().unwrap().unwrap();
      rx.try_recv().unwrap().unwrap();
      rx
    });
    let observer = thread::spawn(move || {
      let first = tx.snapshot();
      let second = tx.snapshot();
      for lane in Three::ALL {
        assert!(
          first.received(lane) <= second.received(lane),
          "received went backward on {lane:?}"
        );
      }
      tx
    });
    let rx = receiver.join().unwrap();
    drop((observer.join().unwrap(), rx));
  });
}

/// A chain for lane B against a take that moves B's only item into the lookahead and leaves it
/// there, taking lane A's instead; the take runs no chain of its own, as a take that unwinds out
/// of its policy does not. The chain reads B where no item is in transit, so it never finds B
/// empty while the item is between the ring and the lookahead, and it wakes B's waiting receiver.
#[test]
fn ma8_a_chain_sees_an_item_moving_into_the_lookahead() {
  use std::sync::atomic::{AtomicBool as Flag, Ordering};

  struct Woken(Flag);
  impl std::task::Wake for Woken {
    fn wake(self: std::sync::Arc<Self>) {
      self.0.store(true, Ordering::SeqCst);
    }
  }

  model(|| {
    let options = Options::from_fn(|_| LaneOptions::new(NonZeroUsize::MIN));
    let (tx, rx) = channel_with::<u8, Three, Stamped>(options, Stamped).unwrap();
    tx.try_send(Three::A, 1).unwrap();
    tx.try_send(Three::B, 2).unwrap();
    let woken = std::sync::Arc::new(Woken(Flag::new(false)));
    let key = tx.shared().lane_at(1).arrivals.listen(
      std::task::Waker::from(woken.clone()),
      loom::sync::Arc::new(crate::arrivals::Receive::new()),
    );
    let taker = {
      let tx = tx.clone();
      thread::spawn(move || {
        let mut slot = None;
        tx.shared()
          .take(&Quota::up_to(1), &mut slot, &mut crate::wake::Staged::new());
        assert_eq!(slot.map(|received| received.lane()), Some(Three::A));
        tx
      })
    };
    let chainer = {
      let tx = tx.clone();
      thread::spawn(move || {
        tx.shared()
          .chain(&Quota::up_to(1).lanes(Three::B..=Three::B));
        tx
      })
    };
    drop((taker.join().unwrap(), chainer.join().unwrap()));
    assert!(
      woken.0.load(Ordering::SeqCst),
      "the chain woke the receiver waiting on lane B"
    );
    drop(tx.shared().lane_at(1).arrivals.leave(key));
    drop((tx, rx));
  });
}

/// A receive of lanes A and B, notified and never polled again, and a receive of lane B alone:
/// commits to A and then B leave neither notice on the first, so the second receives B's item.
#[test]
fn ma10_a_notice_belongs_to_a_receive_not_to_a_lane() {
  model(|| {
    let options = Options::from_fn(|_| LaneOptions::new(NonZeroUsize::MIN));
    let (tx, rx) = channel_with::<u8, Three, Strict>(options, Strict).unwrap();
    let other = rx.clone();
    let mut held = rx.recv();
    assert!(
      core::pin::Pin::new(&mut held)
        .poll(&mut core::task::Context::from_waker(
          core::task::Waker::noop()
        ))
        .is_pending()
    );
    let waiting = thread::spawn(move || {
      let mut batch = Vec::new();
      other
        .recv_many_blocking(&mut batch, Quota::up_to(1).lanes(Three::B..=Three::B))
        .unwrap();
      (batch.len(), other)
    });
    let sender = thread::spawn(move || {
      tx.try_send(Three::A, 1).unwrap();
      tx.try_send(Three::B, 2).unwrap();
      tx
    });
    let (taken, other) = waiting.join().unwrap();
    assert_eq!(taken, 1);
    drop(held);
    drop((sender.join().unwrap(), other, rx));
  });
}
