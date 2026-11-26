//! The waits and the choice of a channel of several lanes.

use core::num::NonZeroUsize;

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

/// MP6 (B2): lane B is full and a sender waits on it; a reserve on lane A completes with no
/// receive.
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

/// MA2 (W2, D3): receiver A admits lane A only, receiver B every lane; a lane-B item is committed:
/// B is woken for it.
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

/// MA5 (W2): a receiver listening on lanes A and B, woken on both, takes one item and discards its
/// other listener; the receiver of lane B alone is woken by the chain for the other item.
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

/// MT1 (O2): one thread commits an express token and then an item; a receiver receives twice: the
/// token first.
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

/// MT2 (D1, D4): two receivers take batches of up to two over three queued items of two lanes:
/// the batches are disjoint, cover all three, and no choice of one falls inside the other's.
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

/// MT3 (O3): sender A commits to lane A and returns; then sender B commits to lane B; a receiver
/// beside them never returns B's item while A's is queued.
#[test]
fn mt3_a_later_commit_to_a_lower_lane_is_not_taken_first() {
  model(|| {
    let (tx, rx) = lanes(false);
    let first_done = Arc::new(AtomicBool::new(false));
    let a = {
      let (tx, first_done) = (tx.clone(), first_done.clone());
      thread::spawn(move || {
        tx.try_send(Three::A, 1).unwrap();
        first_done.store(true, SeqCst);
      })
    };
    let b = thread::spawn(move || {
      while !first_done.load(SeqCst) {
        thread::yield_now();
      }
      tx.try_send(Three::B, 2).unwrap();
      tx
    });
    let receiver = thread::spawn(move || {
      let first = rx.recv_blocking().unwrap().into_item();
      (first, rx)
    });
    a.join().unwrap();
    let tx = b.join().unwrap();
    let (first, rx) = receiver.join().unwrap();
    assert_eq!(first, 1, "B's item was taken while A's was queued");
    drop((tx, rx));
  });
}

/// X2 under loom: every interleaving of two threads, two operations each, over three lanes of
/// capacity 1, is linearizable against the model.
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
