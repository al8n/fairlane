//! One lane, differentially: the channel and the reference model, driven by the same seeded steps
//! on one thread. A pending future is polled only once its waker was woken, as an executor would;
//! after every step the woken futures are polled until none is woken, and then both sides must
//! agree.

use std::{sync::Arc as StdArc, task::Waker};

use super::*;
use crate::{
  Permit, Recv, Reserve,
  model::{Model, PermitId, Reserved, WaiterId},
  policy::Strict,
  testing::Rng,
};

type Policy1 = DefaultPolicy<Single>;

struct Waiting<'a> {
  future: Reserve<'a, u64, Single, Policy1>,
  probe: StdArc<Probe>,
  waker: Waker,
  waiter: WaiterId,
}

struct Held<'a> {
  permit: Permit<'a, u64, Single, Policy1>,
  model: PermitId,
}

struct Receiving<'a> {
  future: Recv<'a, u64, Single, Policy1>,
  probe: StdArc<Probe>,
  waker: Waker,
}

/// The model's answer to one receive of up to one item.
fn model_recv(model: &mut Model<u64, Single, ()>) -> Option<(u64, u64)> {
  model
    .recv_many(&Quota::up_to(1), (), &Strict)
    .pop()
    .map(|taken| (taken.item, taken.ordinal))
}

fn walk(seed: u64, steps: usize) {
  let mut rng = Rng::new(seed);
  let capacity = 1 + rng.below(3);
  let (tx, rx) = bounded::<u64>(cap(capacity)).unwrap();
  let mut model =
    Model::<u64, Single, ()>::new(&Options::from_fn(|Single| LaneOptions::new(cap(capacity))));
  let mut waiting: Vec<Waiting<'_>> = Vec::new();
  let mut held: Vec<Held<'_>> = Vec::new();
  let mut receiving: Vec<Receiving<'_>> = Vec::new();
  let mut next_item = 0_u64;

  for step in 0..steps {
    let at = format!("seed {seed}, step {step}");
    match rng.below(12) {
      0 | 1 => match (tx.try_reserve(Single), model.try_reserve(Single)) {
        (Ok(permit), Ok(id)) => held.push(Held { permit, model: id }),
        (Err(channel), Err(model)) => assert_eq!(channel, model, "try_reserve, {at}"),
        (channel, model) => panic!("try_reserve: {:?} for {model:?}, {at}", channel.err()),
      },
      2 | 3 => {
        let (probe, waker) = Probe::new();
        let mut future = tx.reserve(Single);
        match (poll_with(&mut future, &waker), model.reserve(Single)) {
          (Poll::Ready(Ok(permit)), Reserved::Granted(id)) => held.push(Held { permit, model: id }),
          (Poll::Ready(Err(Closed)), Reserved::Closed) => {}
          (Poll::Pending, Reserved::Waiting(waiter)) => waiting.push(Waiting {
            future,
            probe,
            waker,
            waiter,
          }),
          (channel, model) => panic!("reserve: ready {} for {model:?}, {at}", channel.is_ready()),
        }
      }
      4 if !waiting.is_empty() => {
        let gone = waiting.swap_remove(rng.below(waiting.len()));
        model.leave(gone.waiter);
        drop(gone);
      }
      5 | 6 if !held.is_empty() => {
        let Held { permit, model: id } = held.swap_remove(rng.below(held.len()));
        next_item += 1;
        match (permit.send(next_item), model.send(id, next_item, ())) {
          (Ok(()), Ok(_)) => {}
          (Err(SendError(a)), Err(b)) => assert_eq!(a, b, "{at}"),
          (channel, model) => panic!("send: {channel:?} for {model:?}, {at}"),
        }
      }
      7 if !held.is_empty() => {
        let Held { permit, model: id } = held.swap_remove(rng.below(held.len()));
        drop(permit);
        model.drop_permit(id);
      }
      8 => {
        let mut quota = Quota::up_to(rng.below(4));
        if rng.one_in(4) {
          quota = quota.limit_with(Single, rng.below(3));
        }
        let mut batch = Vec::new();
        let channel = rx.try_recv_many(&mut batch, &quota);
        if !quota.admits(Single) {
          assert_eq!(channel, Err(RecvManyError::EmptyQuota), "{at}");
        } else {
          let ended = model.ended(&quota);
          let taken = model.recv_many(&quota, (), &Strict);
          let ours: Vec<(u64, u64)> = batch
            .into_iter()
            .map(|received| (received.ordinal(), received.into_item()))
            .collect();
          let theirs: Vec<(u64, u64)> = taken.iter().map(|t| (t.ordinal, t.item)).collect();
          assert_eq!(ours, theirs, "try_recv_many, {at}");
          match channel {
            Ok(n) => assert_eq!(n, theirs.len(), "{at}"),
            Err(RecvManyError::Ended) => assert!(ended && theirs.is_empty(), "{at}"),
            Err(RecvManyError::EmptyQuota) => panic!("an admitting quota refused, {at}"),
          }
        }
      }
      9 => {
        let (probe, waker) = Probe::new();
        let mut future = rx.recv();
        let ended = model.ended(&Quota::up_to(1));
        match poll_with(&mut future, &waker) {
          Poll::Ready(Ok(received)) => {
            let ordinal = received.ordinal();
            let item = received.into_item();
            assert_eq!(model_recv(&mut model), Some((item, ordinal)), "recv, {at}");
          }
          Poll::Ready(Err(Ended)) => assert!(ended, "recv ended early, {at}"),
          Poll::Pending => {
            assert_eq!(
              model.counts(Single).queued,
              0,
              "recv slept beside an item, {at}"
            );
            receiving.push(Receiving {
              future,
              probe,
              waker,
            });
          }
        }
      }
      10 if !receiving.is_empty() => {
        drop(receiving.swap_remove(rng.below(receiving.len())));
      }
      11 if rng.one_in(10) => {
        tx.close();
        model.close();
      }
      _ => {}
    }

    // Quiescence: poll every woken future until none is woken.
    loop {
      let mut woke = false;
      let mut still = Vec::new();
      for mut pending in waiting.drain(..) {
        if !pending.probe.take() {
          still.push(pending);
          continue;
        }
        woke = true;
        match (
          poll_with(&mut pending.future, &pending.waker),
          model.verdict(pending.waiter),
        ) {
          (Poll::Pending, None) => still.push(pending),
          (Poll::Ready(Ok(permit)), Some(Ok(id))) => held.push(Held { permit, model: id }),
          (Poll::Ready(Err(Closed)), Some(Err(Closed))) => {}
          (channel, model) => panic!(
            "a woken reserve: ready {} for {model:?}, {at}",
            channel.is_ready()
          ),
        }
      }
      waiting = still;
      let mut still = Vec::new();
      for mut pending in receiving.drain(..) {
        if !pending.probe.take() {
          still.push(pending);
          continue;
        }
        woke = true;
        let ended = model.ended(&Quota::up_to(1));
        match poll_with(&mut pending.future, &pending.waker) {
          Poll::Pending => still.push(pending),
          Poll::Ready(Ok(received)) => {
            let ordinal = received.ordinal();
            let item = received.into_item();
            assert_eq!(
              model_recv(&mut model),
              Some((item, ordinal)),
              "woken recv, {at}"
            );
          }
          Poll::Ready(Err(Ended)) => assert!(ended, "woken recv ended early, {at}"),
        }
      }
      receiving = still;
      if !woke {
        break;
      }
    }

    // Both sides agree.
    for pending in &waiting {
      assert_eq!(
        model.verdict(pending.waiter),
        None,
        "a reserve left waiting, {at}"
      );
    }
    if !receiving.is_empty() {
      assert_eq!(
        model.counts(Single).queued,
        0,
        "a receive sleeps beside a queued item, {at}"
      );
      assert!(
        !model.ended(&Quota::up_to(1)),
        "a receive sleeps at the end, {at}"
      );
    }
    let ours = tx.snapshot();
    let theirs = model.counts(Single);
    assert_eq!(
      (
        ours.queued(Single),
        ours.reserved(Single),
        ours.remaining(Single),
        ours.senders_waiting(Single),
        ours.committed(Single),
        ours.received(Single),
        ours.is_closed(),
      ),
      (
        theirs.queued,
        theirs.reserved,
        theirs.remaining,
        theirs.senders_waiting,
        theirs.committed,
        theirs.received,
        theirs.closed,
      ),
      "snapshot, {at}"
    );
    assert_eq!(ours.receivers_waiting(Single), receiving.len(), "{at}");
  }
  drop(receiving);
  drop(waiting);
  drop(held);
}

#[test]
#[cfg_attr(miri, ignore)]
fn differential_one_lane() {
  for seed in 0..200 {
    walk(seed, 200);
  }
}

#[test]
fn differential_one_lane_one_seed() {
  walk(7, 60);
}
