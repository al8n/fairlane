//! The channel's waits and closes, for one lane.

use core::{
  num::NonZeroUsize,
  pin::Pin,
  task::{Context, Poll},
};

use loom::{
  future::block_on,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering::SeqCst},
  },
  thread,
};

use super::{model, model_with_branches};
use crate::{Ended, Receiver, SendError, Sender, Single, TryReserveError, bounded, error::Closed};

fn channel(capacity: usize) -> (Sender<u8>, Receiver<u8>) {
  bounded(NonZeroUsize::new(capacity).unwrap()).unwrap()
}

/// Polls `future` once, with a waker that does nothing: it joins its line, and a later
/// `block_on` polls it again.
fn poll_once<F: core::future::Future + Unpin>(future: &mut F) -> Poll<F::Output> {
  Pin::new(future).poll(&mut Context::from_waker(core::task::Waker::noop()))
}

/// Two senders, one awaited and one blocking, and one receiver, capacity 1: both items arrive. One
/// item each keeps the model within reach at bound 3; the order of one sender's items is checked
/// by the unit laws, the differential tests and the stress test.
#[test]
fn mp1_two_senders_one_receiver() {
  model(|| {
    let (tx, rx) = channel(1);
    let awaited = {
      let tx = tx.clone();
      thread::spawn(move || {
        block_on(tx.send(Single, 0)).unwrap();
      })
    };
    let blocking = thread::spawn(move || {
      tx.send_blocking(Single, 10).unwrap();
    });
    let receiver = thread::spawn(move || {
      (0..2)
        .map(|_| rx.recv_blocking().unwrap().into_item())
        .collect::<Vec<u8>>()
    });
    awaited.join().unwrap();
    blocking.join().unwrap();
    let mut got = receiver.join().unwrap();
    got.sort_unstable();
    assert_eq!(got, [0, 10]);
  });
}

/// A reserve waits; a receive frees room while a third thread tries to reserve. Where the waiter
/// was in line and had not finished, the late `try_reserve` reads `Full`.
#[test]
fn mp2_a_late_try_reserve_never_passes_a_waiter() {
  model(|| {
    let (tx, rx) = channel(1);
    tx.try_send(Single, 0).unwrap();
    let in_line = Arc::new(AtomicBool::new(false));
    let done = Arc::new(AtomicBool::new(false));
    let waiter = {
      let (tx, in_line, done) = (tx.clone(), in_line.clone(), done.clone());
      thread::spawn(move || {
        let mut reserve = tx.reserve(Single);
        // A reserve that resolved at once is not polled again: a resolved future never wakes.
        let permit = match poll_once(&mut reserve) {
          Poll::Ready(permit) => permit,
          Poll::Pending => {
            in_line.store(true, SeqCst);
            block_on(reserve)
          }
        }
        .unwrap();
        done.store(true, SeqCst);
        drop(permit);
      })
    };
    let late = thread::spawn(move || {
      let saw = in_line.load(SeqCst);
      let taken = tx.try_reserve(Single).map(drop);
      let finished = done.load(SeqCst);
      if saw && !finished {
        assert_eq!(
          taken,
          Err(TryReserveError::Full),
          "a late try_reserve passed a waiter"
        );
      }
    });
    let receiver = thread::spawn(move || {
      rx.recv_blocking().unwrap();
      rx
    });
    waiter.join().unwrap();
    late.join().unwrap();
    drop(receiver.join().unwrap());
  });
}

/// A waiting reserve dropped while the receive that grants it runs: afterwards the room is free.
#[test]
fn mp3_a_dropped_waiter_loses_no_room() {
  model(|| {
    let (tx, rx) = channel(1);
    tx.try_send(Single, 0).unwrap();
    let waiter = {
      let tx = tx.clone();
      thread::spawn(move || {
        let mut reserve = tx.reserve(Single);
        let _ = poll_once(&mut reserve);
        drop(reserve);
      })
    };
    let receiver = thread::spawn(move || {
      rx.recv_blocking().unwrap();
      rx
    });
    waiter.join().unwrap();
    let rx = receiver.join().unwrap();
    assert!(tx.try_reserve(Single).is_ok(), "the room was lost");
    drop(rx);
  });
}

/// A permit dropped unsent while a reserve waits: the waiter is granted.
#[test]
fn mp4_a_dropped_permit_grants_the_waiter() {
  model(|| {
    let (tx, rx) = channel(1);
    let permit = block_on(tx.reserve_owned(Single)).unwrap();
    let waiter = thread::spawn(move || {
      drop(block_on(tx.reserve(Single)).unwrap());
    });
    let dropper = thread::spawn(move || drop(permit));
    dropper.join().unwrap();
    waiter.join().unwrap();
    drop(rx);
  });
}

/// A release races a reserve that is about to wait, through the blocking doors: never room beside
/// a sleeping reserve.
#[test]
fn mp5_a_release_and_a_reserve_about_to_wait() {
  model(|| {
    let (tx, rx) = channel(1);
    tx.try_send(Single, 0).unwrap();
    let waiter = thread::spawn(move || {
      drop(tx.reserve_blocking(Single).unwrap());
      tx
    });
    let receiver = thread::spawn(move || {
      rx.recv_blocking().unwrap();
      rx
    });
    let tx = waiter.join().unwrap();
    let rx = receiver.join().unwrap();
    drop((tx, rx));
  });
}

/// A reserve granted and dropped before it was told, a newcomer, and a later waiter (Codex round
/// 1's second scenario). The grant is the dropped reserve's until it passes it on, so the later
/// waiter is granted, and the lane keeps its one unit.
#[test]
fn mp7_a_dropped_grant_passes_on_to_a_later_waiter() {
  model(|| {
    let (tx, rx) = channel(1);
    tx.try_send(Single, 0).unwrap();
    let dropped = {
      let tx = tx.clone();
      thread::spawn(move || {
        let mut reserve = tx.reserve(Single);
        let _ = poll_once(&mut reserve);
        drop(reserve);
      })
    };
    let receiver = thread::spawn(move || {
      rx.recv_blocking().unwrap();
      rx
    });
    let later = {
      let tx = tx.clone();
      thread::spawn(move || match tx.try_reserve(Single) {
        Ok(newcomer) => {
          let mut waiter = tx.reserve(Single);
          let first = poll_once(&mut waiter);
          drop(newcomer);
          let permit = match first {
            Poll::Ready(permit) => permit,
            Poll::Pending => block_on(waiter),
          };
          drop(permit.unwrap());
        }
        Err(_) => drop(tx.reserve_blocking(Single).unwrap()),
      })
    };
    dropped.join().unwrap();
    later.join().unwrap();
    let rx = receiver.join().unwrap();
    assert!(
      tx.try_reserve(Single).is_ok(),
      "the lane keeps its one unit"
    );
    drop(rx);
  });
}

/// Two receivers wait; one item is committed; one receiver's future is dropped at once, maybe
/// holding the notification: the other receives the item.
#[test]
fn ma1_a_dropped_notified_receiver_passes_the_wake_on() {
  model(|| {
    let (tx, rx) = channel(1);
    let other = rx.clone();
    let listening = Arc::new(AtomicBool::new(false));
    let dropper = {
      let listening = listening.clone();
      thread::spawn(move || {
        let mut recv = rx.recv();
        assert!(poll_once(&mut recv).is_pending());
        listening.store(true, SeqCst);
        drop(recv);
        rx
      })
    };
    let receiver = thread::spawn(move || other.recv_blocking().unwrap().into_item());
    let sender = thread::spawn(move || {
      while !listening.load(SeqCst) {
        thread::yield_now();
      }
      tx.try_send(Single, 5).unwrap();
      tx
    });
    let rx = dropper.join().unwrap();
    assert_eq!(receiver.join().unwrap(), 5);
    drop((sender.join().unwrap(), rx));
  });
}

/// Receiver A takes one of two items (a limit of 1) while receiver B is parked: B is woken for the
/// other.
///
/// One execution of it runs past loom's default of 1000 branches: three actors and two waits
/// each, and each wait takes a lane's lock to join its line, is counted, looks again, and takes the
/// lock again to leave. Every wait loop in the crate yields or parks, so the bound is raised, not
/// the model changed.
#[test]
fn ma3_a_receive_that_leaves_items_wakes_the_next() {
  model_with_branches(20_000, || {
    let (tx, rx) = channel(2);
    let other = rx.clone();
    let a = thread::spawn(move || {
      let mut batch = Vec::new();
      rx.recv_many_blocking(&mut batch, 1).unwrap();
      (batch.len(), rx)
    });
    let b = thread::spawn(move || {
      let mut batch = Vec::new();
      other.recv_many_blocking(&mut batch, 1).unwrap();
      (batch.len(), other)
    });
    let sender = thread::spawn(move || {
      tx.try_send(Single, 1).unwrap();
      tx.try_send(Single, 2).unwrap();
      tx
    });
    let (taken_a, rx) = a.join().unwrap();
    let (taken_b, other) = b.join().unwrap();
    assert_eq!(taken_a + taken_b, 2);
    drop((sender.join().unwrap(), rx, other));
  });
}

/// A receive polled once with one waker, then driven with another, against a commit: the waker's
/// refresh looks at the receive's state again once it holds the new waker's clone, so a
/// notification that landed while the lock was let go is not missed, and the item is received.
#[test]
fn ma6_a_refreshed_receive_takes_the_commit() {
  model(|| {
    let (tx, rx) = channel(1);
    let receiver = thread::spawn(move || {
      let mut recv = rx.recv();
      let received = match poll_once(&mut recv) {
        Poll::Ready(received) => received,
        Poll::Pending => block_on(recv),
      };
      received.unwrap().into_item()
    });
    let sender = thread::spawn(move || {
      tx.try_send(Single, 3).unwrap();
      tx
    });
    assert_eq!(receiver.join().unwrap(), 3);
    drop(sender.join().unwrap());
  });
}

/// Receiver A is notified and its task never polls it; receiver B waits; two items are committed:
/// the second commit notifies B, though A's notice is outstanding, so B receives an item.
#[test]
fn ma9_an_outstanding_notice_holds_back_no_other_receiver() {
  model(|| {
    let (tx, rx) = channel(2);
    let other = rx.clone();
    let mut held = rx.recv();
    assert!(poll_once(&mut held).is_pending());
    let waiting = thread::spawn(move || other.recv_blocking().unwrap().into_item());
    let sender = thread::spawn(move || {
      tx.try_send(Single, 1).unwrap();
      tx.try_send(Single, 2).unwrap();
      tx
    });
    let item = waiting.join().unwrap();
    assert!(item == 1 || item == 2);
    drop(held);
    drop((sender.join().unwrap(), rx));
  });
}

/// A commit against a receiver going to sleep: never an item beside a sleeping receiver that
/// nobody told.
#[test]
fn ma4_a_commit_and_a_receiver_going_to_sleep() {
  model(|| {
    let (tx, rx) = channel(1);
    let receiver = thread::spawn(move || rx.recv_blocking().unwrap().into_item());
    let sender = thread::spawn(move || {
      tx.try_send(Single, 9).unwrap();
      tx
    });
    assert_eq!(receiver.join().unwrap(), 9);
    drop(sender.join().unwrap());
  });
}

/// `close()` against a parked receiver: it resolves `Ended`.
#[test]
fn mc1_a_close_ends_a_parked_receiver() {
  model(|| {
    let (tx, rx) = channel(1);
    let receiver = thread::spawn(move || matches!(rx.recv_blocking(), Err(Ended)));
    let closer = thread::spawn(move || {
      tx.close();
      tx
    });
    assert!(receiver.join().unwrap());
    drop(closer.join().unwrap());
  });
}

/// The last sender's drop against a parked receiver: it resolves `Ended`.
#[test]
fn mc1_the_last_sender_ends_a_parked_receiver() {
  model(|| {
    let (tx, rx) = channel(1);
    let receiver = thread::spawn(move || matches!(rx.recv_blocking(), Err(Ended)));
    let dropper = thread::spawn(move || drop(tx));
    dropper.join().unwrap();
    assert!(receiver.join().unwrap());
  });
}

/// `close()` against a parked reserve: it resolves `Closed`.
#[test]
fn mc2_a_close_resolves_a_parked_reserve() {
  model(|| {
    let (tx, rx) = channel(1);
    tx.try_send(Single, 0).unwrap();
    let closer = tx.clone();
    let waiter = thread::spawn(move || matches!(tx.reserve_blocking(Single), Err(Closed)));
    let closing = thread::spawn(move || {
      closer.close();
      closer
    });
    assert!(waiter.join().unwrap());
    drop((closing.join().unwrap(), rx));
  });
}

/// The last receiver dropped while a reserve waits: it resolves `Closed`.
#[test]
fn mc3_the_last_receiver_resolves_a_parked_reserve() {
  model(|| {
    let (tx, rx) = channel(1);
    tx.try_send(Single, 0).unwrap();
    let waiter = thread::spawn(move || matches!(tx.reserve_blocking(Single), Err(Closed)));
    let dropper = thread::spawn(move || drop(rx));
    dropper.join().unwrap();
    assert!(waiter.join().unwrap());
  });
}

/// A permit's send against a close that then receives to the end: either the send is `Ok` and the
/// receive holds the item, or the item comes back.
#[test]
fn mc4_a_send_and_a_close_meet_on_the_mark() {
  model(|| {
    let (tx, rx) = channel(1);
    let permit = block_on(tx.reserve_owned(Single)).unwrap();
    let sender = thread::spawn(move || permit.send(7));
    let closer = thread::spawn(move || {
      tx.close();
      let mut got = Vec::new();
      while let Ok(received) = rx.recv_blocking() {
        got.push(received.into_item());
      }
      got
    });
    let sent = sender.join().unwrap();
    let got = closer.join().unwrap();
    match sent {
      Ok(()) => assert_eq!(got, [7], "a committed item nobody received"),
      Err(SendError(item)) => {
        assert_eq!(item, 7);
        assert!(got.is_empty());
      }
    }
  });
}

/// A close against a reserve that is dropped, granted or not: afterwards the lane's room is whole.
/// It guards the line's own close and leave: the close settles a grant not taken up, and a dropped
/// reserve that was closed gives nothing back twice. The round-1 line passes it too.
#[test]
fn mc5_a_close_racing_a_dropped_reserve_keeps_the_room() {
  model(|| {
    let (tx, rx) = channel(1);
    tx.try_send(Single, 0).unwrap();
    let dropped = {
      let tx = tx.clone();
      thread::spawn(move || {
        let mut reserve = tx.reserve(Single);
        let _ = poll_once(&mut reserve);
        drop(reserve);
      })
    };
    let receiver = thread::spawn(move || {
      rx.recv_blocking().unwrap();
      rx
    });
    let closer = {
      let tx = tx.clone();
      thread::spawn(move || {
        tx.close();
      })
    };
    dropped.join().unwrap();
    closer.join().unwrap();
    let rx = receiver.join().unwrap();
    let snapshot = tx.snapshot();
    assert_eq!(
      (
        snapshot.remaining(Single),
        snapshot.reserved(Single),
        snapshot.senders_waiting(Single)
      ),
      (1, 0, 0)
    );
    drop(rx);
  });
}

/// A reserve polled once with one waker, then driven with another, against a close: the waker's
/// refresh never misses the close's wake, and the reserve resolves `Closed`.
#[test]
fn mc6_a_refreshed_reserve_resolves_at_a_close() {
  model(|| {
    let (tx, rx) = channel(1);
    tx.try_send(Single, 0).unwrap();
    let closer = tx.clone();
    let waiter = thread::spawn(move || {
      let mut reserve = tx.reserve(Single);
      match poll_once(&mut reserve) {
        Poll::Ready(verdict) => verdict.map(drop),
        Poll::Pending => block_on(reserve).map(drop),
      }
    });
    let closing = thread::spawn(move || {
      closer.close();
      closer
    });
    assert_eq!(waiter.join().unwrap(), Err(Closed));
    drop((closing.join().unwrap(), rx));
  });
}

/// A `closed()` future polled once with one waker, then driven with another, against a close: it
/// resolves, and the channel reads closed.
#[test]
fn mc7_a_refreshed_when_closed_resolves_at_the_close() {
  model(|| {
    let (tx, rx) = channel(1);
    let closer = tx.clone();
    let watcher = thread::spawn(move || {
      {
        let mut closed = tx.closed();
        if poll_once(&mut closed).is_pending() {
          block_on(closed);
        }
      }
      tx
    });
    let closing = thread::spawn(move || {
      closer.close();
      closer
    });
    let tx = watcher.join().unwrap();
    assert!(tx.is_closed());
    drop((tx, closing.join().unwrap(), rx));
  });
}
